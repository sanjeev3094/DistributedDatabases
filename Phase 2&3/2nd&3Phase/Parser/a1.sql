CREATE OR REPLACE PACKAGE BODY emp_actions AS  -- body
  CURSOR desc_salary RETURN EmpRecTyp IS
     SELECT empno, sal FROM emp ORDER BY sal DESC;
  PROCEDURE hire_employee (
     ename  VARCHAR2,
     job    VARCHAR2,
     mgr    NUMBER,
     sal    NUMBER,
     comm   NUMBER,
     deptno NUMBER) IS
  BEGIN
     INSERT INTO emp VALUES (empno_seq.NEXTVAL, ename, job,
        mgr, SYSDATE, sal, comm, deptno);
  END hire_employee;

  PROCEDURE fire_employee (emp_id NUMBER) IS
  BEGIN
     DELETE FROM emp WHERE empno = emp_id;
  END fire_employee;
END emp_actions;
