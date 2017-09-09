create table student2(
	rollNo varchar(30),
	studName varchar(50) NOT NULL,
	prog varchar(20) NOT NULL,
	deptId varchar(10) NOT NULL,
	yoa date NOT NULL,
	yop date,
	emailId varchar(100),
	dob date NOT NULL,
	hometown varchar(50) NOT NULL,
	status varchar(10),
	PRIMARY KEY(rollNo));

create table faculty2(
	facultyId varchar(20),
	salary float NOT NULL,
	hometown varchar(50) NOT NULL,
	emailId varchar(100),
	PRIMARY KEY(facultyId));

create table course2(
	courseId varchar(20),
	courseName varchar(50) NOT NULL,
	credit integer NOT NULL,
	cType varchar(30) NOT NULL,
	prerequisite varchar(20),
	PRIMARY KEY(courseId));

create table enrollment2(
	rollNo varchar(30),
	oId varchar(12),
	grade varchar(4),
	attendance float,
	PRIMARY KEY(rollNo,oId));

create table department(
	deptId varchar(10),
	deptName varchar(30) NOT NULL,
	hodId varchar(20),
	PRIMARY KEY(deptId));


