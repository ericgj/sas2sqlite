libname data 'C:\Users\EGjertsen.ERT\Documents\sas2sqlite\test\fixtures\default';

data data.test;
	input test_date yymmdd10. +1 test_time time8. +1 test_datetime ymddttm19.;
	format test_date date.;
	format test_time time.;
	format test_datetime datetime.;
	datalines;
2021-08-08 01:02:03 
2021-01-01          2021-01-01 00:00:00
           23:59:59 2020-12-31 23:59:59
run;
