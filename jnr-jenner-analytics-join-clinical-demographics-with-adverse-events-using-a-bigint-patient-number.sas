/*---
c:/utl/jnr-jenner-analytics-join-clinical-demographics-with-adverse-events-using-a-bigint-patient-number.sas
 ---*/
 
Too long to pose, see
https://github.com/rogerjdeangelis/jnr-jenner-analytics-join-clinical-demographics-with-adverse-events-using-a-bigint-patient-number
https://github.com/rogerjdeangelis/jnr-alpha-jenner-analytics-extract-postgresql-table-with-bigint-and-process-bigint-with-datastep

Jenner analytics join STDM clinical demographics with adverse events using a bigint patient number

The join was done with a sas dataset merge, not passthru. Parquet files are
easily manipulated by Jenner Analytics. This can also be done using Duckdb csv files.

PROBLEM:  Join dm(demographics) and ae(adverse events) using  64bit integer keys and subset Females
          Column usubjid is a 64bit integer, type bigint
          Drop down to Duckdb Macroon the end

  1. INPUT: duckdb database has SDTM datasets dm(demographics) and ae(adverse events)
           Note the bigint values for usubjid
  2  OUTPUT: duckdb tables as parquet files
  3  Join parquet files
     data workx.dmae;
         merge 'd:\parquet\dm.parquet' 'd:\parquet\ae.parquet';
         by usubjid;
         if sex='F';
     run;

DUCKDB Tables DM and AE

   DM: Demographics
   ┌──────────────────┬────────┬────────┬───────┬─────────┬─────────┬────────────┐
   │     usubjid      │ siteid │ subjid │  age  │  ageu   │   sex   │  BRTHDTC   │
   │      int64       │ int32  │ int32  │ int32 │ varchar │ varchar │  varchar   │
   ├──────────────────┼────────┼────────┼───────┼─────────┼─────────┼────────────┤
   │ 9091119254740991 │ 909111 │  40991 │    67 │ YEARS   │ M       │ 2092-03-15 │
   │ 9091119254740993 │ 909111 │  40993 │    65 │ YEARS   │ M       │ 2091-07-01 │
   │ 9091119254740995 │ 909111 │  40995 │    47 │ YEARS   │ F       │ 2090-11-20 │
   │ 9091119254740997 │ 909111 │  40997 │    43 │ YEARS   │ F       │ 2003-01-10 │
   │ 9091119254741999 │ 909111 │  41999 │    57 │ YEARS   │ M       │ 2003-01-10 │
   └──────────────────┴────────┴────────┴───────┴─────────┴─────────┴────────────┘

   AE: Adverse Events
   ┌──────────────────┬────────┬────────┬───────┬────────┬────────┬────────────────────┐
   │     usubjid      │ siteid │ subjid │ aedy  │ aestdy │ aeendy │       aeterm       │
   │      int64       │ int32  │ int32  │ int32 │ int32  │ int32  │      varchar       │
   ├──────────────────┼────────┼────────┼───────┼────────┼────────┼────────────────────┤
   │ 9091119254740991 │ 909111 │  40991 │   647 │    589 │    589 │ ABNORMAL BEHAVIOUR │
   │ 9091119254740993 │ 909111 │  40993 │   792 │    639 │    700 │ ACUTE PSYCHOSIS    │
   │ 9091119254740995 │ 909111 │  40995 │   639 │    422 │    478 │ AFFECTIVE DISORDER │
   │ 9091119254740997 │ 909111 │  40997 │   715 │    279 │    288 │ AGGRESSION         │
   │ 9091119254741999 │ 909111 │  41999 │   693 │    685 │    818 │ AFFECT LABILITY    │
   └──────────────────┴────────┴────────┴───────┴────────┴────────┴────────────────────┘

   SOLUTION
      data workx.dmae;
       merge 'd:\parquet\dm.parquet' 'd:\parquet\ae.parquet';
       by usubjid;
       if sex='F';
   run;

FINAL OUTPUT:
   proc print data=workx.dmae;
   run;

   Obs           usubjid  siteid  subjid  age   ageu  sex     BRTHDTC  aedy  aestdy  aeendy              aeterm

     1  9091119254740995  909111   40995   47  YEARS  F    2090-11-20   639     422     478  AFFECTIVE DISORDER
     2  9091119254740997  909111   40997   43  YEARS  F    2003-01-10   715     279     288  AGGRESSION


 /******************************************************************************************************************/
 /* CREATE INPUT                                                                                                   */
 /******************************************************************************************************************/

proc datasets lib=workx kill;
run;quit;

%utlfkil(d:\parquet\dm.parquet);
%utlfkil(d:\parquet\ae.parquet);

%slc_submit_duck("
    DROP TABLE IF EXISTS dm;
    CREATE TABLE dm (
        usubjid BIGINT ,
        siteid int,
        subjid int ,
        age int,
        ageu varchar,
        sex varchar,
        BRTHDTC varchar
    );

    INSERT INTO dm VALUES
        (9091119254740991,909111,40991,67,'YEARS','M','2092-03-15'),
        (9091119254740993,909111,40993,65,'YEARS','M','2091-07-01'),
        (9091119254740995,909111,40995,47,'YEARS','F','2090-11-20'),
        (9091119254740997,909111,40997,43,'YEARS','F','2003-01-10'),
        (9091119254741999,909111,41999,57,'YEARS','M','2003-01-10');

    select * from dm;

    COPY dm TO 'd:\parquet\dm.parquet' (FORMAT PARQUET);

    DROP TABLE IF EXISTS ae;
    CREATE TABLE ae (
        usubjid BIGINT PRIMARY KEY,
        siteid int,
        subjid int,
        aedy int,
        aestdy int,
        aeendy int,
        aeterm varchar
        );

    INSERT INTO ae VALUES
        (9091119254740991,909111,40991, 647, 589, 589, 'ABNORMAL BEHAVIOUR'),
        (9091119254740993,909111,40993, 792, 639, 700, 'ACUTE PSYCHOSIS  ' ),
        (9091119254740995,909111,40995, 639, 422, 478, 'AFFECTIVE DISORDER'),
        (9091119254740997,909111,40997, 715, 279, 288, 'AGGRESSION'        ),
        (9091119254741999,909111,41999, 693, 685, 818, 'AFFECT LABILITY'   );

    select * from ae;

    COPY ae TO 'd:\parquet\ae.parquet' (FORMAT PARQUET);
    "
    ,duckdb=c:/temp/mydb.duckdb
    );

   DM: Demographicss
   ┌──────────────────┬────────┬────────┬───────┬─────────┬─────────┬────────────┐
   │     usubjid      │ siteid │ subjid │  age  │  ageu   │   sex   │  BRTHDTC   │
   │      int64       │ int32  │ int32  │ int32 │ varchar │ varchar │  varchar   │
   ├──────────────────┼────────┼────────┼───────┼─────────┼─────────┼────────────┤
   │ 9091119254740991 │ 909111 │  40991 │    67 │ YEARS   │ M       │ 2092-03-15 │
   │ 9091119254740993 │ 909111 │  40993 │    65 │ YEARS   │ M       │ 2091-07-01 │
   │ 9091119254740995 │ 909111 │  40995 │    47 │ YEARS   │ F       │ 2090-11-20 │
   │ 9091119254740997 │ 909111 │  40997 │    43 │ YEARS   │ F       │ 2003-01-10 │
   │ 9091119254741999 │ 909111 │  41999 │    57 │ YEARS   │ M       │ 2003-01-10 │
   └──────────────────┴────────┴────────┴───────┴─────────┴─────────┴────────────┘

   AE: Adverse Events
   ┌──────────────────┬────────┬────────┬───────┬────────┬────────┬────────────────────┐
   │     usubjid      │ siteid │ subjid │ aedy  │ aestdy │ aeendy │       aeterm       │
   │      int64       │ int32  │ int32  │ int32 │ int32  │ int32  │      varchar       │
   ├──────────────────┼────────┼────────┼───────┼────────┼────────┼────────────────────┤
   │ 9091119254740991 │ 909111 │  40991 │   647 │    589 │    589 │ ABNORMAL BEHAVIOUR │
   │ 9091119254740993 │ 909111 │  40993 │   792 │    639 │    700 │ ACUTE PSYCHOSIS    │
   │ 9091119254740995 │ 909111 │  40995 │   639 │    422 │    478 │ AFFECTIVE DISORDER │
   │ 9091119254740997 │ 909111 │  40997 │   715 │    279 │    288 │ AGGRESSION         │
   │ 9091119254741999 │ 909111 │  41999 │   693 │    685 │    818 │ AFFECT LABILITY    │
   └──────────────────┴────────┴────────┴───────┴────────┴────────┴────────────────────┘


/******************************************************************************************************************/
/* PROCESS                                                                                                        */
/******************************************************************************************************************/

data workx.dmae;
    merge 'd:\parquet\dm.parquet' 'd:\parquet\ae.parquet';
    by usubjid;
    if sex='F';
run;
proc print data=workx.dmae;
run;

/******************************************************************************************************************/
/* FINAL OUTPUT: Female adverse events                                                                            */
/******************************************************************************************************************/


  Obs           usubjid  siteid  subjid  age   ageu  sex     BRTHDTC  aedy  aestdy  aeendy              aeterm

    1  9091119254740995  909111   40995   47  YEARS  F    2090-11-20   639     422     478  AFFECTIVE DISORDER
    2  9091119254740997  909111   40997   43  YEARS  F    2003-01-10   715     279     288  AGGRESSION


 /******************************************************************************************************************/
 /* DROP DOWN RO DUCKDB MACRO                                                                                      */
 /******************************************************************************************************************/
filename ft15f001 "c:/wpsoto/slc_submit_duck.sas";
parmcards4;
%macro slc_submit_duck(
       pgm
      ,duckdb=c:/temp/dbase.duckdb
      ,return=  /* name for the macro variable from duckdb */
      )/des="Semi colon separated set of Powershell commands - drop down to Powershell. Bactic converted to double quote";

   %utlfkil(c:/temp/pgm.sql);
   %utlfkil(c:/temp/duck1.log);

   * write the program to a temporary file;
   filename py_pgm "c:/temp/pgm.sql" lrecl=32756 recfm=v;
   data _null_;
     length pgm  $32755 ;
     file py_pgm ;
     pgm=compbl(&pgm);
     if index(pgm,"`") then
        cmd=translate(pgm,"22"x,"`");
     put pgm;
     putlog pgm;
   run;quit;

   filename rut pipe "duckdb &duckdb -f c:/temp/pgm.sql ";

   data _null_;
     file print;
     infile rut;
     input;
     put _infile_;
     putlog _infile_;
   run;

   filename rut clear;
   filename py_pgm clear;
   * use the clipboard to create macro variable;
   %if "&return" ^= "" %then %do;
     filename clp clipbrd ;
     data _null_;
      length txt $200;
      infile clp;
      input;
      putlog "*******  " _infile_;
      call symputx("&return",_infile_,"G");
     run;quit;
   %end;
%mend slc_submit_duck;
;;;;
run;

/******************************************************************************************************************/
/* END                                                                                                            */
/******************************************************************************************************************/

