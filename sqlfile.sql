SELECT S.sname
FROM Sailors S, Reserves R
WHERE S.sid=R.sid and R.day='2009-12-21'
;
