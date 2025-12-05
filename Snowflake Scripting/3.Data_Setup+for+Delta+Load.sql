-- create required database and schema
CREATE DATABASE IF NOT EXISTS EMP;
CREATE SCHEMA IF NOT EXISTS EMP.STAGING;
CREATE SCHEMA IF NOT EXISTS EMP.HRDATA;
CREATE SCHEMA IF NOT EXISTS EMP.PROCS;
CREATE SCHEMA IF NOT EXISTS EMP.WORK;

CREATE TABLE EMP.WORK.SP_ERROR_LOGS
(PROC_NAME VARCHAR, ERROR_TYPE VARCHAR, ERROR_CODE VARCHAR, ERROR_MESSAGE VARCHAR, SQL_STATE VARCHAR);

-- create staging and target tables
CREATE OR REPLACE TABLE emp.staging.employees
(   employee_id NUMBER(6)
   ,first_name VARCHAR(20)
   ,last_name VARCHAR(25) NOT NULL
   ,email VARCHAR(25) NOT NULL
   ,phone_number VARCHAR(20)
   ,hire_date DATE NOT NULL
   ,job_id VARCHAR(10) NOT NULL
   ,salary NUMBER(8,2)
   ,commission_pct NUMBER(2,2)
   ,manager_id NUMBER(6)
   ,dept_id NUMBER(4)
);

CREATE OR REPLACE TABLE emp.hrdata.employees
(   employee_id NUMBER(6)
   ,first_name VARCHAR(20)
   ,last_name VARCHAR(25) NOT NULL
   ,email VARCHAR(25) NOT NULL
   ,phone_number VARCHAR(20)
   ,hire_date DATE NOT NULL
   ,job_id VARCHAR(10) NOT NULL
   ,salary NUMBER(8,2)
   ,commission_pct NUMBER(2,2)
   ,manager_id NUMBER(6)
   ,dept_id NUMBER(4)
   ,PRIMARY KEY (employee_id)
);

-- insert data into staging table
INSERT INTO emp.staging.employees VALUES 
   ( 100,  'Ramana',  'Rao',  'RRAO',  '420.271.4567',  TO_DATE('17-JUN-1987', 'dd-MON-yyyy'),  'ADM_PRES',  24000,  NULL,  NULL,  90),
   ( 101,  'Devi',  'Kapoor',  'DKAPOOR',  '420.271.4568',  TO_DATE('21-SEP-1989', 'dd-MON-yyyy'),  'ADM_VP',  17000,  NULL,  100,  90),
   ( 102,  'Nagesh',  'Reddy',  'RNAGESH',  '420.271.4569',  TO_DATE('13-JAN-1993', 'dd-MON-yyyy'),  'ADM_VP',  17000,  NULL,  100,  90),
   ( 103,  'Ranga',  'Rayudu',  'RRANGA',  '590.423.4567',  TO_DATE('03-JAN-1990', 'dd-MON-yyyy'),  'IT_PRG',  9000,  NULL,  102,  60),
   ( 104,  'Sivarama',  'Krishna',  'SKRISHNA',  '590.423.4568',  TO_DATE('21-MAY-1991', 'dd-MON-yyyy'),  'IT_PRG',  6000,  NULL,  103,  60),
   ( 105,  'Shoban',  'Reddy',  'RSHOBAN',  '590.423.4569',  TO_DATE('25-JUN-1997', 'dd-MON-yyyy'),  'IT_PRG',  4800,  NULL,  103,  60),
   ( 106,  'Jamuna',  'Kumari',  'KJAMUNA',  '590.423.4560',  TO_DATE('05-FEB-1998', 'dd-MON-yyyy'),  'IT_PRG',  4800,  NULL,  103,  60),
   ( 107,  'Jaya',  'Prakash',  'PJAYA',  '590.423.5567',  TO_DATE('07-FEB-1999', 'dd-MON-yyyy'),  'IT_PRG',  4200,  NULL,  103,  60),
   ( 108,  'Raja',  'Krishna',  'RKRISHNA',  '613.234.4569',  TO_DATE('17-AUG-1994', 'dd-MON-yyyy'),  'GI_MGR',  12000,  NULL,  101,  100),
   ( 109,  'Mohan',  'Chandra',  'MCHANDRA',  '613.234.4169',  TO_DATE('16-AUG-1994', 'dd-MON-yyyy'),  'GI_ACCOUNT',  9000,  NULL,  108,  100),
   ( 110,  'Mohan',  'Venkata',  'VMOHAN',  '613.234.4269',  TO_DATE('28-SEP-1997', 'dd-MON-yyyy'),  'GI_ACCOUNT',  8200,  NULL,  108,  100),
   ( 111,  'Sudha',  'Rani',  'RSUDHA',  '613.234.4369',  TO_DATE('30-SEP-1997', 'dd-MON-yyyy'),  'GI_ACCOUNT',  7700,  NULL,  108,  100),
   ( 112,  'Raja',  'Shekar',  'RSHEKAR',  '613.234.4469',  TO_DATE('07-MAR-1998', 'dd-MON-yyyy'),  'GI_ACCOUNT',  7800,  NULL,  108,  100),
   ( 113,  'Siva Sankar',  'Prasad',  'SPRASAD',  '613.234.4567',  TO_DATE('07-DEC-1999', 'dd-MON-yyyy'),  'GI_ACCOUNT',  6900,  NULL,  108,  100),
   ( 114,  'Simha',  'Reddy',  'RSIMHA',  '715.327.4561',  TO_DATE('07-DEC-1994', 'dd-MON-yyyy'),  'BU_MAN',  11000,  NULL,  100,  30),
   ( 115,  'Venkat',  'Rana',  'RVENKAT',  '715.327.4562',  TO_DATE('18-MAY-1995', 'dd-MON-yyyy'),  'BU_CLERK',  3100,  NULL,  114,  30),
   ( 116,  'Ramya',  'Sagar',  'RSAGAR',  '715.327.4563',  TO_DATE('24-DEC-1997', 'dd-MON-yyyy'),  'BU_CLERK',  2900,  NULL,  114,  30),
   ( 117,  'Shiva',  'Nagaraj',  'SNAGARAJ',  '715.327.4564',  TO_DATE('24-JUL-1997', 'dd-MON-yyyy'),  'BU_CLERK',  2800,  NULL,  114,  30),
   ( 118,  'Raja Babu',  'A',  'ARAJABABU',  '715.327.4565',  TO_DATE('15-NOV-1998', 'dd-MON-yyyy'),  'BU_CLERK',  2600,  NULL,  114,  30),
   ( 119,  'Meena',  'Chowdary',  'KCOLMENA',  '715.327.4566',  TO_DATE('10-AUG-1999', 'dd-MON-yyyy'),  'BU_CLERK',  2500,  NULL,  114,  30),
   ( 120,  'Vijaya',  'Ramu',  'RVIJAYA',  '470.123.1234',  TO_DATE('18-JUL-1996', 'dd-MON-yyyy'),  'TA_MAN',  8000,  NULL,  100,  50),
   ( 121,  'Roja',  'Kumari',  'RKUMARI',  '470.123.2234',  TO_DATE('10-APR-1997', 'dd-MON-yyyy'),  'TA_MAN',  8200,  NULL,  100,  50),
   ( 122,  'Relangi',  'Ramana',  'RRELANGI',  '470.123.3234',  TO_DATE('01-MAY-1995', 'dd-MON-yyyy'),  'TA_MAN',  7900,  NULL,  100,  50),
   ( 123,  'Jyothi',  'Chandran',  'CJYOTHI',  '470.123.4234',  TO_DATE('10-OCT-1997', 'dd-MON-yyyy'),  'TA_MAN',  6500,  NULL,  100,  50),
   ( 124,  'Brmaha',  'Kasula',  'BKASULA',  '470.123.5234',  TO_DATE('16-NOV-1999', 'dd-MON-yyyy'),  'TA_MAN',  5800,  NULL,  100,  50),
   ( 125,  'Lalitha',  'Matham',  'LMATHAM',  '470.124.1214',  TO_DATE('16-JUL-1997', 'dd-MON-yyyy'),  'TA_CLERK',  3200,  NULL,  120,  50),
   ( 126,  'Viswanath',  'Sharma',  'SSHARMA',  '470.124.1224',  TO_DATE('28-SEP-1998', 'dd-MON-yyyy'),  'TA_CLERK',  2700,  NULL,  120,  50),
   ( 127,  'Narayana',  'Reddy',  'RNARAYANA',  '470.124.1334',  TO_DATE('14-JAN-1999', 'dd-MON-yyyy'),  'TA_CLERK',  2400,  NULL,  120,  50),
   ( 128,  'Vijaya',  'Krishna',  'VKRISHNA',  '470.124.1434',  TO_DATE('08-MAR-2000', 'dd-MON-yyyy'),  'TA_CLERK',  2200,  NULL,  120,  50),
   ( 129,  'Raghuram',  'K',  'KRAGURAM',  '470.124.5234',  TO_DATE('20-AUG-1997', 'dd-MON-yyyy'),  'TA_CLERK',  3300,  NULL,  121,  50),
   ( 130,  'Sharwa',  'Kumar',  'KSHARWA',  '470.124.6234',  TO_DATE('30-OCT-1997', 'dd-MON-yyyy'),  'TA_CLERK',  2800,  NULL,  121,  50),
   ( 131,  'Siddarth',  'Anand',  'SANAND',  '470.124.7234',  TO_DATE('16-FEB-1997', 'dd-MON-yyyy'),  'TA_CLERK',  2500,  NULL,  121,  null),
   ( 132,  'Rashmi',  'Maheshan',  'RMAHESHAN',  '470.124.8234',  TO_DATE('10-APR-1999', 'dd-MON-yyyy'),  'TA_CLERK',  2100,  NULL,  121,  50),
   ( 133,  'Anjali',  'SV',  'SVANJALI',  '470.127.1934',  TO_DATE('14-JUN-1996', 'dd-MON-yyyy'),  'TA_CLERK',  3300,  NULL,  122,  50),
   ( 134,  'Chaitanya',  'AN',  'ANCHAITANYA',  '470.127.1834',  TO_DATE('26-AUG-1998', 'dd-MON-yyyy'),  'TA_CLERK',  2900,  NULL,  122,  50),
   ( 135,  'Mahesh',  'Guntur',  'GMAHESH',  '470.127.1734',  TO_DATE('12-DEC-1999', 'dd-MON-yyyy'),  'TA_CLERK',  2400,  NULL,  122,  50),
   ( 136,  'Sonali',  'Sharma',  'SSONALI',  '470.127.1634',  TO_DATE('06-FEB-2000', 'dd-MON-yyyy'),  'TA_CLERK',  2200,  NULL,  122,  50),
   ( 137,  'Charan',  'Raj',  'RCHARAN',  '470.121.1234',  TO_DATE('14-JUL-1995', 'dd-MON-yyyy'),  'TA_CLERK',  3600,  NULL,  123,  50),
   ( 138,  'Arjun',  'Allam',  'AARJUN',  '470.121.2034',  TO_DATE('26-OCT-1997', 'dd-MON-yyyy'),  'TA_CLERK',  3200,  NULL,  123,  50),
   ( 139,  'Varun',  'Sharma',  'SVARUN',  '470.121.2019',  TO_DATE('12-FEB-1998', 'dd-MON-yyyy'),  'TA_CLERK',  2700,  NULL,  123,  50),
   ( 140,  'Samantha',  'Patil',  'SPATIL',  '470.121.1834',  TO_DATE('06-APR-1998', 'dd-MON-yyyy'),  'TA_CLERK',  2500,  NULL,  123,  50),
   ( 141,  'Tarun',  'Kumar',  'KTARUN',  '470.121.8009',  TO_DATE('17-OCT-1995', 'dd-MON-yyyy'),  'TA_CLERK',  3500,  NULL,  124,  50),
   ( 142,  'Pranavi',  'Rajesh',  'RPRANAVI',  '470.121.2994',  TO_DATE('29-JAN-1997', 'dd-MON-yyyy'),  'TA_CLERK',  3100,  NULL,  124,  50),
   ( 143,  'Deepika',  'Manne',  'MDEEPIKA',  '470.121.2874',  TO_DATE('15-MAR-1998', 'dd-MON-yyyy'),  'TA_CLERK',  2600,  NULL,  124,  50),
   ( 144,  'Sudheer',  'Babu',  'BSUDHEER',  '470.121.2004',  TO_DATE('09-JUL-1998', 'dd-MON-yyyy'),  'TA_CLERK',  2500,  NULL,  124,  50),
   ( 145,  'Sumanth',  'Eluru',  'ESUMANTH',  '091.044.429268',  TO_DATE('01-OCT-1996', 'dd-MON-yyyy'),  'SL_MAN',  14000,  .4,  100,  80),
   ( 146,  'Pavan',  'Kumar',  'KPAVAN',  '091.044.467268',  TO_DATE('05-JAN-1997', 'dd-MON-yyyy'),  'SL_MAN',  13500,  .3,  100,  80),
   ( 147,  'Amritha',  'Rao',  'RAMRITHA',  '091.044.429278',  TO_DATE('10-MAR-1997', 'dd-MON-yyyy'),  'SL_MAN',  12000,  .3,  100,  80),
   ( 148,  'Rahul',  'Varma',  'VRAHUL',  '091.044.619268',  TO_DATE('15-OCT-1999', 'dd-MON-yyyy'),  'SL_MAN',  11000,  .3,  100,  80),
   ( 149,  'Rohit',  'Kumar',  'RKUMAR',  '091.044.429018',  TO_DATE('29-JAN-2000', 'dd-MON-yyyy'),  'SL_MAN',  10500,  .2,  100,  80),
   ( 150,  'Ganesh',  'Karthik',  'GKARTHIK',  '091.044.129268',  TO_DATE('30-JAN-1997', 'dd-MON-yyyy'),  'SL_REP',  10000,  .3,  145,  80),
   ( 151,  'Suman',  'Krishnamoorthy',  'KSUMAN',  '091.044.345268',  TO_DATE('24-MAR-1997', 'dd-MON-yyyy'),  'SL_REP',  9500,  .25,  145,  80),
   ( 152,  'Preethi',  'Ganta',  'GPREETHI',  '091.044.478968',  TO_DATE('20-AUG-1997', 'dd-MON-yyyy'),  'SL_REP',  9000,  .25,  145,  80),
   ( 153,  'Shobana',  'Kunche',  'KSHOBANA',  '091.044.498718',  TO_DATE('30-MAR-1998', 'dd-MON-yyyy'),  'SL_REP',  8000,  .2,  145,  80),
   ( 154,  'Srikanth',  'Koneru',  'KSRIKANTH',  '091.044.987668',  TO_DATE('09-DEC-1998', 'dd-MON-yyyy'),  'SL_REP',  7500,  .2,  145,  80),
   ( 155,  'Swathi',  'Bandaru',  'BSWATHI',  '091.044.486508',  TO_DATE('23-NOV-1999', 'dd-MON-yyyy'),  'SL_REP',  7000,  .15,  145,  80),
   ( 156,  'Vinod',  'Kumar',  'VKUMAR',  '091.040.429268',  TO_DATE('30-JAN-1996', 'dd-MON-yyyy'),  'SL_REP',  10000,  .35,  146,  80),
   ( 157,  'Anand',  'Sully',  'PSULLY',  '091.040.929268',  TO_DATE('04-MAR-1996', 'dd-MON-yyyy'),  'SL_REP',  9500,  .35,  146,  80),
   ( 158,  'Kamalesh',  'GK',  'GKKAMAL',  '091.040.829268',  TO_DATE('01-AUG-1996', 'dd-MON-yyyy'),  'SL_REP',  9000,  .35,  146,  80),
   ( 159,  'Rajani',  'Balagam',  'BRAJANI',  '091.040.729268',  TO_DATE('10-MAR-1997', 'dd-MON-yyyy'),  'SL_REP',  8000,  .3,  146,  80),
   ( 160,  'Radha',  'Chowdary',  'CRADHA',  '091.040.629268',  TO_DATE('15-DEC-1997', 'dd-MON-yyyy'),  'SL_REP',  7500,  .3,  146,  80),
   ( 161,  'Sarath',  'Paderu',  'PSARATH',  '091.040.529268',  TO_DATE('03-NOV-1998', 'dd-MON-yyyy'),  'SL_REP',  7000,  .25,  146,  80),
   ( 162,  'Anushka',  'Verma',  'VANUSHKA',  '091.080.129268',  TO_DATE('11-NOV-1997', 'dd-MON-yyyy'),  'SL_REP',  10500,  .25,  147,  80),
   ( 163,  'Geetha',  'Subramanyam',  'SGEETHA',  '091.080.229268',  TO_DATE('19-MAR-1999', 'dd-MON-yyyy'),  'SL_REP',  9500,  .15,  147,  80),
   ( 164,  'Vinay',  'Jain',  'JVINAY',  '091.080.329268',  TO_DATE('24-JAN-2000', 'dd-MON-yyyy'),  'SL_REP',  7200,  .10,  147,  80),
   ( 165,  'Subbalakshmi',  'Arise',  'ASUBBA',  '091.080.529268',  TO_DATE('23-FEB-2000', 'dd-MON-yyyy'),  'SL_REP',  6800,  .1,  147,  null),
   ( 166,  'Santosh',  'Ande',  'ASANTOSH',  '091.080.629268',  TO_DATE('24-MAR-2000', 'dd-MON-yyyy'),  'SL_REP',  6400,  .10,  147,  80),
   ( 167,  'Amit',  'Tripathi',  'TAMIT',  '091.080.729268',  TO_DATE('21-APR-2000', 'dd-MON-yyyy'),  'SL_REP',  6200,  .10,  147,  80),
   ( 168,  'Mallesh',  'Yadav',  'MYADAV',  '091.40.929268',  TO_DATE('11-MAR-1997', 'dd-MON-yyyy'),  'SL_REP',  11500,  .25,  148,  80),
   ( 169 ,  'Harish',  'Manikonda',  'MHARISH',  '091.40.829268',  TO_DATE('23-MAR-1998', 'dd-MON-yyyy'),  'SL_REP',  10000,  .20,  148,  80),
   ( 170,  'Tarun',  'Chakravarthi',  'CTARUN',  '091.40.729268',  TO_DATE('24-JAN-1998', 'dd-MON-yyyy'),  'SL_REP',  9600,  .20,  148,  80),
   ( 171,  'Arjun',  'Bandi',  'BARJUN',  '091.40.629268',  TO_DATE('23-FEB-1999', 'dd-MON-yyyy'),  'SL_REP',  7400,  .15,  148,  80),
   ( 172,  'Sunitha',  'Karam',  'KSUNITHA',  '091.40.529268',  TO_DATE('24-MAR-1999', 'dd-MON-yyyy'),  'SL_REP',  7300,  .15,  148,  80),
   ( 173,  'Veena',  'Singam',  'SVEENA',  '091.40.329268',  TO_DATE('21-APR-2000', 'dd-MON-yyyy'),  'SL_REP',  6100,  .10,  148,  80),
   ( 174,  'Gayathri',  'Rao',  'RGAYATHRI',  '091.40.429267',  TO_DATE('11-MAY-1996', 'dd-MON-yyyy'),  'SL_REP',  11000,  .30,  149,  80),
   ( 175,  'Jagannath',  'Venkata',  'JVENKATA',  '091.40.429266',  TO_DATE('19-MAR-1997', 'dd-MON-yyyy'),  'SL_REP',  8800,  .25,  149,  80),
   ( 176,  'Seetha',  'Kumari',  'KSEETHA',  '091.40.429265',  TO_DATE('24-MAR-1998', 'dd-MON-yyyy'),  'SL_REP',  8600,  .20,  149,  80),
   ( 177,  'Tara',  'Trivedi',  'TTARA',  '091.40.429264',  TO_DATE('23-APR-1998', 'dd-MON-yyyy'),  'SL_REP',  8400,  .20,  149,  80),
   ( 178,  'Veerendra',  'Sagar',  'VSAGAR',  '091.40.429263',  TO_DATE('24-MAY-1999', 'dd-MON-yyyy'),  'SL_REP',  7000,  .15,  149,  NULL),
   ( 179,  'Ramesh',  'Nelluri',  'NRAMESH',  '091.40.429262',  TO_DATE('04-JAN-2000', 'dd-MON-yyyy'),  'SL_REP',  6200,  .10,  149,  80),
   ( 180,  'Trisha',  'Varma',  'VTRISHA',  '804.5569876',  TO_DATE('24-JAN-1998', 'dd-MON-yyyy'),  'SL_CLERK',  3200,  NULL,  120,  50),
   ( 181,  'Chandra',  'Sekharan',  'SCHANDRA',  '804.5569877',  TO_DATE('23-FEB-1998', 'dd-MON-yyyy'),  'SL_CLERK',  3100,  NULL,  120,  50),
   ( 182,  'Keerthi',  'Reddy',  'RKEERTHI',  '804.5569878',  TO_DATE('21-JUN-1999', 'dd-MON-yyyy'),  'SL_CLERK',  2500,  NULL,  120,  50),
   ( 183,  'Priya',  'Darshini',  'DPRIYA',  '804.5569879',  TO_DATE('03-FEB-2000', 'dd-MON-yyyy'),  'SL_CLERK',  2800,  NULL,  120,  50),
   ( 184,  'Vamsi',  'Allam',  'AVAMSI',  '470.509.1876',  TO_DATE('27-JAN-1996', 'dd-MON-yyyy'),  'SL_CLERK',  4200,  NULL,  121,  50),
   ( 185,  'Preethi',  'Singh',  'SPREETHI',  '470.509.2876',  TO_DATE('20-FEB-1997', 'dd-MON-yyyy'),  'SL_CLERK',  4100,  NULL,  121,  50),
   ( 186,  'Nabagata',  'Das',  'DNABAGATA',  '470.509.3876',  TO_DATE('24-JUN-1998', 'dd-MON-yyyy'),  'SL_CLERK',  3400,  NULL,  121,  50),
   ( 187,  'Manohar',  'Konda',  'KMANOHAR',  '470.509.4876',  TO_DATE('07-FEB-1999', 'dd-MON-yyyy'),  'SL_CLERK',  3000,  NULL,  121,  50),
   ( 188,  'Anita',  'Vemuri',  'VANITA',  '470.505.1876',  TO_DATE('14-JUN-1997', 'dd-MON-yyyy'),  'SL_CLERK',  3800,  NULL,  122,  50),
   ( 189,  'Karthik',  'Dandu',  'DKARTHIK',  '470.505.2876',  TO_DATE('13-AUG-1997', 'dd-MON-yyyy'),  'SL_CLERK',  3600,  NULL,  122,  50),
   ( 190,  'Sree',  'Lakshmi',  'SLAKSHMI',  '470.505.3876',  TO_DATE('11-JUL-1998', 'dd-MON-yyyy'),  'SL_CLERK',  2900,  NULL,  122,  50),
   ( 191,  'Arunkumar',  'Degala',  'DARUNKUMAR',  '470.505.4876',  TO_DATE('19-DEC-1999', 'dd-MON-yyyy'),  'SL_CLERK',  2500,  NULL,  122,  50),
   ( 192,  'Vani',  'Raghuram',  'VRAGHURAM',  '470.501.1876',  TO_DATE('04-FEB-1996', 'dd-MON-yyyy'),  'SL_CLERK',  4000,  NULL,  123,  50),
   ( 193,  'Nagendra',  'Tirumala',  'TNAGENDRA',  '470.501.2876',  TO_DATE('03-MAR-1997', 'dd-MON-yyyy'),  'SL_CLERK',  3900,  NULL,  123,  50),
   ( 194,  'Sharada',  'Ravindran',  'RSHARADA',  '470.501.3876',  TO_DATE('01-JUL-1998', 'dd-MON-yyyy'),  'SL_CLERK',  3200,  NULL,  123,  50),
   ( 195,  'Amarnath',  'Gunta',  'GAMARNATH',  '470.501.4876',  TO_DATE('17-MAR-1999', 'dd-MON-yyyy'),  'SL_CLERK',  2800,  NULL,  123,  50),
   ( 196,  'Balaram',  'Naguri',  'NBALARAM',  '804.5569811',  TO_DATE('24-APR-1998', 'dd-MON-yyyy'),  'SL_CLERK',  3100,  NULL,  124,  50),
   ( 197,  'Amala',  'Patel',  'PAMALA',  '804.5569822',  TO_DATE('23-MAY-1998', 'dd-MON-yyyy'),  'SL_CLERK',  3000,  NULL,  124,  50),
   ( 198,  'Divya',  'Venkata',  'VDIVYA',  '804.5569833',  TO_DATE('21-JUN-1999', 'dd-MON-yyyy'),  'SL_CLERK',  2600,  NULL,  124,  50),
   ( 199,  'Suresh',  'Naidu',  'NSURESH',  '804.5569844',  TO_DATE('13-JAN-2000', 'dd-MON-yyyy'),  'SL_CLERK',  2600,  NULL,  124,  50),
   ( 200,  'Lokesh',  'Kavuri',  'KLOKESH',  '420.271.4444',  TO_DATE('17-SEP-1987', 'dd-MON-yyyy'),  'ADM_ASST',  4400,  NULL,  101,  10),
   ( 201,  'Sirisha',  'Pnatham',  'PSIRISHA',  '420.271.5555',  TO_DATE('17-FEB-1996', 'dd-MON-yyyy'),  'MG_MAN',  13000,  NULL,  100,  20),
   ( 202,  'Swapna',  'Kumari',  'KSWAPNA',  '503.820.6666',  TO_DATE('17-AUG-1997', 'dd-MON-yyyy'),  'MG_REP',  6000,  NULL,  201,  20),
   ( 203,  'Bhargav',  'Amalapuram',  'ABHARGAV',  '420.271.7777',  TO_DATE('07-JUN-1994', 'dd-MON-yyyy'),  'HR_REPO',  6500,  NULL,  101,  40),
   ( 204,  'Surendra',  'Bapatla',  'BSURENDRA',  '420.271.8888',  TO_DATE('07-JUN-1994', 'dd-MON-yyyy'),  'PR_REPO',  10000,  NULL,  101,  70),
   ( 205,  'Sandeep',  'Koneti',  'KSANDEEP',  '420.271.8080',  TO_DATE('07-JUN-1994', 'dd-MON-yyyy'),  'VC_MGR',  12000,  NULL,  101,  110),
   ( 206,  'Bhavya',  'Ramesh',  'RBHAVYA',  '420.271.8181',  TO_DATE('07-JUN-1994', 'dd-MON-yyyy'),  'VC_ACCOUNT',  8300,  NULL,  205,  110
   );