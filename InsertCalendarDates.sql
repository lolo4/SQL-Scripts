begin
create table #Calendar (
	[DateID] int not null
	,[DateISO] date not null
	,[DayNameEnglish] varchar(9) not null
	,[DayNameShortEnglish] varchar(3) not null
	,[DayOfTheWeek] tinyint not null
	,[DayOfTheMonth] tinyint not null
	,[DayOfTheQuarter] tinyint not null
	,[DayOfTheYear] smallint not null
	,[WeekOfTheYear] tinyint not null
	,[MonthNameEnglish] varchar(9) not null
	,[MonthNameShortEnglish] varchar(3) not null
	,[MonthOfTheQuarter] tinyint not null
	,[MonthOfTheYear] tinyint not null
	,[QuarterOfTheYear] tinyint not null
	,[Year] int not null
	,[YearShort] smallint not null
	,[YearMonth] int not null
	,[IsWeekend] bit not null
	,[FirstDayOfThisMonth] date not null
	,[LastDayOfThisMonth] date not null
	,[FirstDayOfThisQuarter] date not null
	,[LastDayOfThisQuarter] date not null
	,[FirstDayOfThisYear] date not null
	,[LastDayOfThisYear] date not null
	,[FirstDayOfPreviousMonth] date not null
	,[LastDayOfPreviousMonth] date not null
	,[FirstDayOfPreviousQuarter] date not null
	,[LastDayOfPreviousQuarter] date not null
	,[FirstDayOfPreviousYear] date not null
	,[LastDayOfPreviousYear] date not null
	,[FirstDayOfNextMonth] date not null
	,[LastDayOfNextMonth] date not null
	,[FirstDayOfNextQuarter] date not null
	,[LastDayOfNextQuarter] date not null
	,[FirstDayOfNextYear] date not null
	,[LastDayOfNextYear] date not null
	,[PublicHolidayEngland] bit not null
	,[BusinessPeriod] int null
	,[FinancialPeriod] int null
	,[CreatedDate] datetime not null
);

alter table #calendar
add constraint [pk_Calendar_DateID] primary key ([DateID])
;

alter table #calendar
add constraint [pk_Calendar_CreatedDate] default (current_timestamp) for [CreatedDate]
;
end

begin
declare
	@startDate date
	,@endDate date
;

set @startDate = '20160101'; -- enter start date
set @endDate = '20171231'; -- enter end date

	while (@startDate <= @endDate)
	if not exists (select [DateID] from #Calendar where [DateISO] = @startDate)
	begin
		begin
			insert into #Calendar (
				[DateID]
				,[DateISO]
				,[DayNameEnglish]
				,[DayNameShortEnglish]
				,[DayOfTheWeek]
				,[DayOfTheMonth]
				,[DayOfTheQuarter]
				,[DayOfTheYear]
				,[WeekOfTheYear]
				,[MonthNameEnglish]
				,[MonthNameShortEnglish]
				,[MonthOfTheQuarter]
				,[MonthOfTheYear]
				,[QuarterOfTheYear]
				,[Year]
				,[YearShort]
				,[YearMonth]
				,[IsWeekend]
				,[FirstDayOfThisMonth]
				,[LastDayOfThisMonth]
				,[FirstDayOfThisQuarter]
				,[LastDayOfThisQuarter]
				,[FirstDayOfThisYear]
				,[LastDayOfThisYear]
				,[FirstDayOfPreviousMonth]
				,[LastDayOfPreviousMonth]
				,[FirstDayOfPreviousQuarter]
				,[LastDayOfPreviousQuarter]
				,[FirstDayOfPreviousYear]
				,[LastDayOfPreviousYear]
				,[FirstDayOfNextMonth]
				,[LastDayOfNextMonth]
				,[FirstDayOfNextQuarter]
				,[LastDayOfNextQuarter]
				,[FirstDayOfNextYear]
				,[LastDayOfNextYear]
				,[PublicHolidayEngland]
			)

			select
				convert(char(8),replace(replace(@startDate,'-',''),'/',''),120) as [DateID]
				,cast(@startDate as date) as [DateISO]
				,datename(dw,@startDate) as [DayNameEnglish]
				,left(datename(dw,@startDate),3) as [DayNameShortEnglish]
				,datepart(dw,@startDate) as [DayOfTheWeek]
				,datename(dd,@startDate) as [DayOfTheMonth]
				,(datediff(d, dateadd(qq, datediff(qq, 0, @startDate), 0), @startDate) + 1) as [DayOfTheQuarter]
				,datepart(dy,@startDate) as [DayOfTheYear]
				,datepart(isoww,@startDate) as [WeekOfTheYear]
				,datename(mm,@startDate) as [MonthNameEnglish]
				,left(datename(mm,@startDate),3) as [MonthNameShortEnglish]
				,case
					when (datepart(mm,@startDate) = 1) then 1
					when (datepart(mm,@startDate) = 2) then 2
					when (datepart(mm,@startDate) = 3) then 3
					when (datepart(mm,@startDate) = 4) then 1
					when (datepart(mm,@startDate) = 5) then 2
					when (datepart(mm,@startDate) = 6) then 3
					when (datepart(mm,@startDate) = 7) then 1
					when (datepart(mm,@startDate) = 8) then 2
					when (datepart(mm,@startDate) = 9) then 3
					when (datepart(mm,@startDate) = 10) then 1
					when (datepart(mm,@startDate) = 11) then 2
					when (datepart(mm,@startDate) = 12) then 3
				end as [MonthOfTheQuarter]
				,datepart(mm,@startDate) as [MonthOfTheYear]
				,datepart(qq,@startDate) as [QuarterOfTheYear]
				,datepart(yy,@startDate) as [Year]
				,right(datepart(yy,@startDate),2) as [YearShort]
				,((datepart(yy,@startDate)*100) + datepart(mm,@startDate)) as [YearMonth]
				,case
					when (datename(dw,@startDate) in ('saturday','sunday')) then 1
					else 0
				end as [IsWeekend]
				,dateadd(dd,1,eomonth(dateadd(mm,-1,@startDate))) as [FirstDayOfThisMonth]
				,eomonth(@startDate) as [LastDayOfThisMonth]
				,dateadd(qq, datediff(qq, 0, @startDate), 0) as [FirstDayOfThisQuarter]
				,dateadd (dd, -1, dateadd(qq, datediff(qq, 0, @startDate) +1, 0)) as [LastDayOfThisQuarter]
				,dateadd(yy, datediff(yy, 0, @startDate), 0) as [FirstDayOfThisYear]
				,dateadd(yy, datediff(yy, 0, @startDate) + 1, -1) as [LastDayOfThisYear]
				,dateadd(dd,1,eomonth(dateadd(mm,-1,dateadd(mm,-1,@startDate)))) as [FirstDayOfPreviousMonth]
				,eomonth(dateadd(mm,-1,@startDate)) as [LastDayOfPreviousMonth]
				,dateadd(qq, datediff(qq, 0, @startDate) - 1, 0) as [FirstDayOfPreviousQuarter]
				,dateadd(dd, -1, dateadd(qq, datediff(qq, 0, @startDate), 0)) as [LastDayOfPreviousQuarter]
				,dateadd(yy,-1,dateadd(yy, datediff(yy, 0, @startDate), 0)) as [FirstDayOfPreviousYear]
				,dateadd(yy,-1,dateadd(yy, datediff(yy, 0, @startDate) + 1, -1)) as [LastDayOfPreviousYear]
				,dateadd(dd,1,eomonth(@startDate)) as [FirstDayOfNextMonth]
				,eomonth(dateadd(mm,1,@startDate)) as [LastDayOfNextMonth]
				,dateadd(qq, datediff(qq, 0, @startDate) + 1, 0) as [FirstDayOfNextQuarter]
				,dateadd (dd, -1, dateadd(qq, datediff(qq, 0, @startDate) +2, 0)) as [LastDayOfNextQuarter]
				,dateadd(yy,1,dateadd(yy, datediff(yy, 0, @startDate), 0)) as [FirstDayOfNextYear]
				,dateadd(yy,1,dateadd(yy, datediff(yy, 0, @startDate) + 1, -1)) as [LastDayOfNextYear]
				,0 as [PublicHolidayEngland]
		end

		begin
			-- Add a day onto the current date for the next loop.
			set @startDate = dateadd(dd,1,@startDate);
		end
	end
end

begin
	select
		*
	from #Calendar
	;
end

begin
	drop table #Calendar;
end

