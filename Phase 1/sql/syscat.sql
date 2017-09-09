create database syscat;
use syscat;

create table site(
	siteId integer PRIMARY KEY,
	siteName varchar(20) NOT NULL,
	ipAddress varchar(20) NOT NULL,
	schemaName varchar(20) NOT NULL,
	userName varchar(20) NOT NULL,
	passwd varchar(20));

create table relation(
	relId integer,
	relName varchar(20),
	PRIMARY KEY(relId));

create table relDetail(
	relId integer NOT NULL,
	attr varchar(30),
	attrType varchar(20) NOT NULL,
	isPKey bool NOT NULL,
	isNullable bool NOT NULL,
	PRIMARY KEY(relId,attr));

create table fragment(
	fragName varchar(30),
	relId integer,
	siteId integer,
	fragType varchar(10) NOT NULL,
	PRIMARY KEY(fragName,relId,siteId));

create table hFrag(
	fragName varchar(30),
	attr varchar(30) NOT NULL,
	operator varchar(30) NOT NULL,
	val varchar(30) NOT NULL,
	PRIMARY KEY(fragName));

create table vFrag(
	fragName varchar(30),
	fragAttr varchar(30),
	PRIMARY KEY(fragName,fragAttr));

CREATE TABLE dFrag(
  fragName VARCHAR(15),
  joinHFrag VARCHAR(50) NOT NULL,
  semiJoinAttr VARCHAR(50) NOT NULL,
  PRIMARY KEY (fragName));






