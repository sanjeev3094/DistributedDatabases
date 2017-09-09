insert into site values(1,"site1","10.3.3.37","syscat","root","openstack");
insert into site values(2,"site2","10.1.66.75","syscat","root","openstack");
insert into site values(3,"site3","10.3.3.214","syscat","root","openstack");

insert into relation values(1,"student");
insert into relation values(2,"faculty");
insert into relation values(3,"course");
insert into relation values(4,"courseOffering");
insert into relation values(5,"enrollment");
insert into relation values(6,"department");


insert into relDetail values(1,"rollNo","varchar(30)",true,false);
insert into relDetail values(1,"studName","varchar(50)",false,false);
insert into relDetail values(1,"prog","varchar(20)",false,false);
insert into relDetail values(1,"deptId","varchar(10)",false,false);
insert into relDetail values(1,"yoa","date",false,false);
insert into relDetail values(1,"yop","date",false,true);
insert into relDetail values(1,"emailId","varchar(100)",false,true);
insert into relDetail values(1,"dob","date",false,false);
insert into relDetail values(1,"hometown","varchar(50)",false,false);
insert into relDetail values(1,"status","varchar(10)",false,true);

insert into relDetail values(2,"facultyId","varchar(20)",true,false);
insert into relDetail values(2,"facultyName","varchar(50)",false,false);
insert into relDetail values(2,"deptId","varchar(10)",false,false);
insert into relDetail values(2,"facultyTitle","varchar(50)",false,false);
insert into relDetail values(2,"salary","float",false,false);
insert into relDetail values(2,"hometown","varchar(50)",false,false);
insert into relDetail values(2,"emailId","varchar(100)",false,true);

insert into relDetail values(3,"courseId","varchar(20)",true,false);
insert into relDetail values(3,"courseName","varchar(50)",false,false);
insert into relDetail values(3,"credit","integer",false,false);
insert into relDetail values(3,"cType","varchar(30)",false,false);
insert into relDetail values(3,"prerequisite","varchar(20)",false,true);

insert into relDetail values(4,"oId","varchar(12)",true,false);
insert into relDetail values(4,"courseId","varchar(20)",false,false);
insert into relDetail values(4,"semester","varchar(30)",false,false);
insert into relDetail values(4,"oYear","integer",false,false);
insert into relDetail values(4,"facultyId","varchar(20)",false,false);
insert into relDetail values(4,"roomNo","varchar(20)",false,true);
insert into relDetail values(4,"meetDay","integer",false,false);
insert into relDetail values(4,"meetAt","time",false,true);

insert into relDetail values(5,"rollNo","varchar(30)",true,false);
insert into relDetail values(5,"oId","varchar(12)",true,false);
insert into relDetail values(5,"grade","varchar(4)",false,true);
insert into relDetail values(5,"attendance","float",false,true);

insert into relDetail values(6,"deptId","varchar(10)",true,false);
insert into relDetail values(6,"deptName","varchar(30)",false,false);
insert into relDetail values(6,"hodId","varchar(20)",false,true);

insert into relation values(1,"student");
insert into relation values(2,"faculty");
insert into relation values(3,"course");
insert into relation values(4,"courseOffering");
insert into relation values(5,"enrollment");
insert into relation values(6,"department");


insert into fragment values("student1",1,1,"HR");
insert into fragment values("student2",1,2,"HR");
insert into fragment values("student3",1,3,"HR");
insert into fragment values("faculty1",2,1,"VR");
insert into fragment values("faculty2",2,2,"VR");
insert into fragment values("course1",3,1,"HR");
insert into fragment values("course2",3,2,"HR");
insert into fragment values("course3",3,3,"HR");
insert into fragment values("courseOffering1",4,1,"VR");
insert into fragment values("courseOffering2",4,3,"VR");
insert into fragment values("enrollment1",5,1,"DR");
insert into fragment values("enrollment2",5,2,"DR");
insert into fragment values("enrollment3",5,3,"DR");
insert into fragment values("department",6,2,"NF");

insert into hFrag values("student1","prog","=","'BTech'");
insert into hFrag values("student2","prog","=","'MTech'");
insert into hFrag values("student3","prog","=","'PhD'");
insert into hFrag values("course1","cType","=","'system'");
insert into hFrag values("course2","cType","=","'foundation'");
insert into hFrag values("course3","cType","=","'elective'");

insert into vFrag values("faculty1","facultyId");
insert into vFrag values("faculty1","facultyName");
insert into vFrag values("faculty1","deptId");
insert into vFrag values("faculty1","facultyTitle");
insert into vFrag values("faculty2","facultyId");
insert into vFrag values("faculty2","salary");
insert into vFrag values("faculty2","hometown");
insert into vFrag values("faculty2","emailId");
insert into vFrag values("courseOffering1","oId");
insert into vFrag values("courseOffering1","courseId");
insert into vFrag values("courseOffering1","semester");
insert into vFrag values("courseOffering1","oYear");
insert into vFrag values("courseOffering2","oId");
insert into vFrag values("courseOffering2","facultyId");
insert into vFrag values("courseOffering2","roomNo");
insert into vFrag values("courseOffering2","meetAt");
insert into vFrag values("courseOffering2","meetDay");

insert into dFrag values("enrollment1","student1","rollNo");
insert into dFrag values("enrollment2","student2","rollNo");
insert into dFrag values("enrollment3","student3","rollNo");

