SELECT S.name, S.id
FROM sailors S, reserves R
WHERE S.id=R.id and R.day='2017-03-05'
;
