use [msdb]
go

/*
--	================================================================================
	Name:	dbo.usp_SQLAgentJobNotification
	Description:
		This stored procedure is to be used as a generic way to send out email 
		notifications to the relevant parties to inform them of a successful or 
		failed SQL Agent job execution.
		This store procedure will also aim to include any information relating to
		the error if the job was executing packages.
--	================================================================================
*/

create proc dbo.usp_SQLAgentJobNotification

	@jobName varchar(max)
	,@recipients varchar(max) = null
	,@subject varchar(max) = null
	,@DBMailProfile varchar(max) = null

as

begin -- start of proc
	declare
		@outcome varchar(max)
		,@jobExecutionStatus varchar(max)
		,@jobRun nvarchar(max)
		,@duration nvarchar(max)
		,@runDate int
		,@runTime int
		,@message nvarchar(max)
		,@body nvarchar(max)
		,@xml nvarchar(max)
		,@instanceID int
		,@executionDuration bigint
	;

	begin -- Start: set up defaults for variables that have not been given a value.
		if ((@DBMailProfile is null) or (@DBMailProfile = ''))
		begin -- Start: set the @DBMailProfile variable if it is blank.
			set @DBMailProfile = (select top 1 [name] from msdb.dbo.sysmail_profile);
		end -- End: set the @DBMailProfile variable if it is blank.
	end -- End: set up defaults for variables that have not been given a value.

	begin -- Start: check the last execution of the job.
	/*
		Find the last execution for the SQL Agent job in question and assign the details of the job
		to the relevant variables for the SP.
	*/
		with lastJobOutcome as (
			select
				j.[name] as [JobName]
				,row_number() over(partition by j.[job_id] order by jh.[run_date] desc, jh.[run_time] desc) as [ExecutionOrderedByDescendingDate]
				,jh.[instance_id] as [InstanceID]
			from msdb.dbo.sysjobs j
				join msdb.dbo.sysjobhistory jh
					on jh.[job_id] = j.[job_id]
			where
				jh.[step_id] = 0
				and j.[name] = @jobName
		)

		select
			@instanceID = [InstanceID]
		from lastJobOutcome
		where
			[ExecutionOrderedByDescendingDate] = 1
		;
	end -- End: check the last execution of the job.

	begin -- Start: Create a log table to hold the relevant logs for the job.
		create table #jobLogs (
			[ID] bigint not null identity(1,1)
			,[JobID] nvarchar(max) null
			,[Name] nvarchar(max) null
			,[InstanceID] int null
			,[StepID] int null
			,[StepName] nvarchar(max) null
			,[Message] nvarchar(max) null
			,[RunStatus] int null
			,[Status] nvarchar(max) null
			,[RunDate] int null
			,[RunTime] int null
			,[RunDuration] int null
			,[Server] nvarchar(max) null
			,[ExecutionDateTime] datetime null
		)
		;
	end -- End: Create a log table to hold the relevant logs for the job.

	begin -- Start: Insert the relevant job logs into the #jobLogs table.
		insert into #jobLogs 
			([JobID],[Name],[InstanceID],[StepID],[StepName],[Message],[RunStatus],[Status],[RunDate],[RunTime],[RunDuration],[Server],[ExecutionDateTime])
		select
			j.[job_id]
			,j.[name]
			,jh.[instance_id]
			,jh.[step_id]
			,jh.[step_name]
			,jh.[message]
			,jh.[run_status]
			,case (jh.run_status)
				when 0 then 'Failed'
				when 1 then 'Succeeded'
				when 2 then 'Retry'
				when 3 then 'Canceled'
				when 4 then 'In Progress'
				else 'Unknown'
			end as [status]
			,jh.[run_date]
			,jh.[run_time]
			,jh.[run_duration]
			,jh.[server]
			,msdb.dbo.agent_datetime(run_date,run_time) as [ExecutionDateTime]
		from msdb.dbo.sysjobs j
			join msdb.dbo.sysjobhistory jh
				on jh.[job_id] = j.[job_id]
		where
			jh.[instance_id] > @instanceID
			and j.[name] = @jobName
	end -- End: Insert the relevant job logs into the #jobLogs table.

	begin -- Start: Set the relevant variables with the information about the current execution.
	/*
		Find out the outcome of the current execution to see whether there were any errors. Exclude any steps
		listed in the tb_JobStepExclusions table.
		Set the duration of the current execution and convert into hours, minutes and seconds.
	*/
		set @outcome = (case
					when (select
							count(jl.[Status]) as [total]
						from #jobLogs jl
						where
							jl.[Status] in ('Failed','Retry','Canceled','Unknown')
							and not exists (
								select
									1
								from msdb.dbo.tb_JobStepExclusions jse
								where
									jse.[stepID] = jl.[StepID]
							)
					) > 0
						then 'Problems'
					else
						'Succeeded'
				end);
		
		set @executionDuration = (select sum([RunDuration]) as [TotalExecutionDuration] from #jobLogs);

		set @duration = (convert(nvarchar(max),(@executionDuration / 10000)) + ' hours, ' 
				+ convert(nvarchar(max),(@executionDuration / 100 % 100)) + ' minutes, '
				+ convert(nvarchar(max),(@executionDuration % 100)) + ' seconds');

		set @jobRun = '''' + @jobName + '''' + ' was run on ' + convert(nvarchar(max),format(CONVERT(date, CONVERT(varchar(8), (select min([RunDate]) as [RunDate] from #jobLogs)), 120),'dd/MM/yyyy'))
				+ ' at ' + convert(nvarchar(max),left(cast((select min([ExecutionDateTime]) as [ExecutionDateTime] from #jobLogs) as time),8))
	end -- End: Set the relevant variables with the information about the current execution.

	begin -- Start: check the outcome of the job execution and send the relevant email.
	/*
		Notify the relevant parties whether the job has succeeded, been cancelled or failed.
	*/
		if (@outcome = 'Succeeded')
		begin -- Start: if job succeeds then send success email.
			if ((@subject is null) or (@subject = ''))
			begin
				set @subject = 'The job succeeded: ' + @jobName + ' completed.';
			end

			set @body = '<p>JOB RUN: ' + @jobRun + '<br>'
				+ '<p>DURATION: ' + @duration + '<br>'
				+ '<p>STATUS: ' + @outcome + '<br>'
				+ '</p>'
			;
			
			if ((@recipients is null) or (@recipients = ''))
			begin -- Start: set the @recipients variable if it is blank.
			-- Send the successful emails to a different email address than the service desk email address.
				set @recipients = 'ITDepartment@Hamleys.co.uk';
			end -- End: set the @recipients variable if it is blank.

			exec msdb.dbo.sp_send_dbmail
				@profile_name = @DBMailProfile
				,@recipients = @recipients
				,@body = @body
				,@subject = @subject
				,@body_format = 'HTML'
			;
		end -- End: if job succeeds then send success email.

		if (@outcome in ('Problems'))
		begin -- Start: if the job fails then send a failure email.
			begin -- start: create temp tables
			/*
				create a temporary table to hold the package names and one to hold the package error information.
			*/
				if OBJECT_ID('tempdb..#jobPackages') is not null
				begin
					drop table #jobPackages;
				end

				create table #jobPackages (
					[ID] int not null identity(1,1)
					,[JobStep] int null
					,[PackageName] varchar(max) collate Latin1_General_CI_AS null
				)
				;

				if OBJECT_ID('tempdb..#packageExecutionInfo') is not null
				begin
					drop table #packageExecutionInfo;
				end

				create table #packageExecutionInfo (
					[ID] int not null identity(1,1)
					,[PackageName] varchar(max) collate Latin1_General_CI_AS null
					,[PackageLocation] varchar(max) collate Latin1_General_CI_AS null
					,[PackageExecutionStatus] varchar(500) collate Latin1_General_CI_AS null
					,[ExecutionStartTime] datetimeoffset null
					,[ProjectVersionNumber] varchar(500) null
					,[ExecutedAs] varchar(max) null
					,[ServerName] varchar(max) null
					,[SQLInstanceName] varchar(max) null
					,[OperationMessageType] varchar(max) null
					,[Message] varchar(max) null
				)
				;
			end -- end: create temp tables

			begin -- start: insert package names into temp table
			/*
				insert all the package names associated with the specified job into the temporary table.
			*/
				insert into #jobPackages ([JobStep],[PackageName])
				select
					js.step_id as [JobStep]
					,case
						when ([subsystem] = 'SSIS') 
							then 
								reverse(
									substring(reverse(substring([command],0,CHARINDEX('.dtsx',[command],0)+5))
									,0
									,charindex('\',reverse(substring([command],0,CHARINDEX('.dtsx',[command],0)+5)),0))
								)
						else
							[subsystem]
					end as [PackageName]
				from msdb.dbo.sysjobs j
					join msdb.dbo.sysjobsteps js
						on js.[job_id] = j.[job_id]
				where
					j.[name] = @jobName
				order by
					j.[job_id], js.[step_id]
				;
			end -- end: insert package names into temp table

			begin -- Start: insert package error messages into temp table
			/*
				Check the error messages associated with the package and insert them into the 
				#packageExecutionInfo table
			*/
				with packageExecutions as (
					select
						e.[execution_id] as [PackageExecutionID]
						,e.[package_name] as [PackageName]
						,(e.[folder_name] + ' / ' + e.[project_name] + ' / ' + e.[package_name]) as [PackageLocation]
						,case (ei.[status])
							when 1 then 'Created'
							when 2 then 'Running'
							when 3 then 'Canceled'
							when 4 then 'Failed'
							when 5 then 'Pending'
							when 6 then 'Ended Unexpectedly'
							when 7 then 'Succeeded'
							when 8 then 'Stopping'
							when 9 then 'Completed'
						end as [PackageExecutionStatus]
						,ei.[start_time] as [ExecutionStartTime]
						,ei.[end_time] as [ExecutionEndTime]
						,e.[project_lsn] as [ProjectVersionNumber]
						,e.[executed_as_name] as [ExecutedAs]
						,case (e.[use32bitruntime])
							when 1 then '32bit'
							when 0 then '64bit'
						end as [x86orx64]
						,o.[stopped_by_name] as [ExecutionStoppedBy]
						,o.[machine_name] as [ServerName]
						,o.[server_name] as [SQLInstanceName]
						,case (ei.[operation_type])
							when 1 then 'Integration Services initialization'
							when 2 then 'Retention window SQL Agent Job'
							when 3 then 'MaxProjectVersion SQL Agent Job'
							when 101 then 'deploy_project Stored Procedure'
							when 106 then 'restore_project Stored Procedure'
							when 200 then 'create_execution or start_execution Stored Procedure'
							when 202 then 'stop_operation Stored Procedure'
							when 300 then 'validate_project Stored Procedure'
							when 301 then 'validate_package Stored Procedure'
							when 1000 then 'configure_catalog Stored Procedure'
						end as [OperationType]
						,om.[operation_message_id] as [OperationMessageID]
						,om.[message_time] as [OperationMessageTime]
						,case (om.[message_type])
							when -1 then 'Unknown'
							when 120 then 'Error'
							when 110 then 'Warning'
							when 70 then 'Information'
							when 10 then 'Pre-validate'
							when 20 then 'Post-validate'
							when 30 then 'Pre-execute'
							when 40 then 'Post-execute'
							when 60 then 'Progress'
							when 50 then 'StatusChange'
							when 100 then 'QueryCancel'
							when 130 then 'TaskFailed'
							when 90 then 'Diagnostic'
							when 200 then 'Custom'
							when 140 then 'DiagnosticEx'
							when 400 then 'NonDiagnostic'
							when 80 then 'VariableValueChanged'
						end as [OperationMessageType]
						,case (om.[message_source_type])
							when 10 then 'Entry APIs, such as T-SQL and CLR Stored procedures'
							when 20 then 'External process used to run package (ISServerExec.exe)'
							when 30 then 'Package-level objects'
							when 40 then 'Control Flow tasks'
							when 50 then 'Control Flow containers'
							when 60 then 'Data Flow task'
						end as [OperationSourceType]
						,om.[message]
						,dense_rank() over(partition by e.[package_name] order by ei.[start_time] desc) as [ExecutionOrderByMostRecent]
					from [SSISDB].[internal].[executions] e
						join [SSISDB].[internal].[operations] o
							on e.[execution_id] = o.[operation_id]
						join [SSISDB].[internal].[execution_info] ei
							on ei.[execution_id] = e.[execution_id]
						join [SSISDB].[catalog].[operation_messages] om
							on om.[operation_id] = e.[execution_id]
					where
						ei.[start_time] > dateadd(dd,-32,current_timestamp)
				)

				insert into #packageExecutionInfo (
					[PackageName]
					,[PackageLocation]
					,[PackageExecutionStatus]
					,[ExecutionStartTime]
					,[ProjectVersionNumber]
					,[ExecutedAs]
					,[ServerName]
					,[SQLInstanceName]
					,[OperationMessageType]
					,[Message]
				)
				select
					pe.[PackageName]
					,pe.[PackageLocation]
					,pe.[PackageExecutionStatus]
					,pe.[ExecutionStartTime]
					,pe.[ProjectVersionNumber]
					,pe.[ExecutedAs]
					,pe.[ServerName]
					,pe.[SQLInstanceName]
					,pe.[OperationMessageType]
					,pe.[Message]
				from packageExecutions pe
				where
					pe.[ExecutionOrderByMostRecent] = 1
					and [OperationMessageType] in ('Error','Unknown','TaskFailed','QueryCancel')
					and exists (
						select
							1
						from #jobPackages jp
						where
							pe.[PackageName] = jp.[PackageName] collate Latin1_General_CI_AS
					)
				;
			end -- End: insert package error messages into temp table

			begin -- Start: send out error messages with email notification.
				if ((@subject is null) or (@subject = ''))
				begin
					set @subject = 'The job has encountered problems: ' + @jobName;
				end

				if ((@recipients is null) or (@recipients = ''))
				begin -- Start: set the @recipients variable if it is blank.
				-- Send the problem emails to the service desk email address.
					set @recipients = 'ServiceDesk@Hamleys.co.uk';
				end -- End: set the @recipients variable if it is blank.
				
				if exists (select * from #packageExecutionInfo)
				begin
					set @xml = 
						cast(
							(select
								[PackageName] as 'td',''
								,[PackageExecutionStatus] as 'td',''
								,[OperationMessageType] as 'td',''
								,[Message] as 'td',''
								,[ExecutionStartTime] as 'td',''
								,[PackageLocation] as 'td',''
								,[ExecutedAs] as 'td',''
								,[ServerName] as 'td',''
								,[SQLInstanceName] as 'td',''
							from #packageExecutionInfo
							for xml path('tr'), elements
							)
						as nvarchar(max))
					;

					set @body = '<html>'
						+ '<p>JOB RUN: ' + @jobRun + '<br>'
						+ '<p>DURATION: ' + @duration + '<br>'
						+ '<p>STATUS: ' + @outcome + '<br>'
						+ '</p>'
						+ '<p></p>'
						+ '<p>Please see the table below for more information.</p>
						<p></p>
						<p></p>
						<body><H1>Execution Table Errors</H1>
						<table border = 1>
						<tr>
						<th>[PackageName]</th> <th>[PackageExecutionStatus]</th> <th>[OperationMessageType]</th> <th>[Message]</th> 
						<th>[ExecutionStartTime]</th> <th>[PackageLocation]</th> <th>[ExecutedAs]</th> <th>[ServerName]</th> <th>[SQLInstanceName]</th>
						</tr>';

					set @body = @body + @xml +'</table></body></html>';
				end
				else
				begin
					set @body = '<html>'
						+ '<p>JOB RUN: ' + @jobRun + '<br>'
						+ '<p>DURATION: ' + @duration + '<br>'
						+ '<p>STATUS: ' + @outcome + '<br>'
						+ '</p>'
						+ '<p>No package error messages have been found to add to this email.</p>
						</html>'
					;
				end

				exec msdb.dbo.sp_send_dbmail
					@profile_name = @DBMailProfile
					,@recipients = @recipients
					,@body = @body
					,@subject = @subject
					,@body_format = 'HTML'
				;
			end -- End: send out error messages with email notification.
		end -- End: if the job fails then send a failure email
	end -- End: check the outcome of the job execution and send the relevant email.
end -- end of proc

















