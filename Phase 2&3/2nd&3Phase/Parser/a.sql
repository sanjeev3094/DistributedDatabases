SELECT empno, sal FROM aaa,emp ORDER BY sal DESC;
CREATE table emp(
     ename  VARCHAR2,
     job    VARCHAR2,
     mgr    NUMBER,
     sal    NUMBER,
     comm   NUMBER,
     deptno NUMBER);
 
INSERT INTO emp VALUES (mgr, SYSDATE, sal, comm, deptno);

DELETE FROM emp WHERE empno = emp_id;
update Eimp set a=null;
