DROP VIEW IF EXISTS q0, q1i, q1ii, q1iii, q1iv, q2i, q2ii, q2iii, q3i, q3ii, q3iii, q4i, q4ii, q4iii, q4iv, q4v;

-- Question 0
CREATE VIEW q0(era) 
AS
  SELECT MAX(era)
  FROM pitching
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM master
  WHERE weight > 300
;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM master
  WHERE namefirst LIKE '% %'
;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count)
AS
  SELECT birthyear, avg(height) as avgheight, count(*) as c
  FROM master
  GROUP BY birthyear
  ORDER BY birthyear
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count)
AS
  SELECT birthyear, avg(height) as avgheight, count(*) as c
  FROM master
  GROUP BY birthyear
  HAVING avg(height) > 70
  ORDER BY birthyear
;

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid)
AS
  SELECT namefirst, namelast, MTable.playerid, yearid
  FROM master as MTable INNER JOIN halloffame as HTable 
       	      	 	ON MTable.playerID = Htable.playerID
  WHERE inducted = 'Y'
  ORDER BY yearid DESC
;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid)
AS
  SELECT namefirst, namelast, MTable.playerid, STable.schoolid,  HTable.yearid
  FROM master as MTable INNER JOIN halloffame as HTable ON MTable.playerid = HTable.playerid
  INNER JOIN collegeplaying as CTable ON HTable.playerid = CTable.playerid
  INNER JOIN schools as STable ON CTable.schoolid = STable.schoolid 
  WHERE STable.schoolState = 'CA' AND HTable.inducted = 'Y'
  ORDER BY HTable.yearid DESC, STable.schoolid, MTable.playerid
;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
AS
  SELECT MTable.playerid, namefirst, namelast, CTable.schoolid
  FROM (master as MTable INNER JOIN halloffame as HTable ON MTable.playerid = HTable.playerid)
  LEFT JOIN collegeplaying as CTable ON HTable.playerid = CTable.playerid
  WHERE HTable.inducted = 'Y' AND (CTable.schoolid IS NULL OR CTable.schoolid IS NOT NULL)
  ORDER BY MTable.playerid DESC, CTable.schoolid
;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
  SELECT MTable.playerid, namefirst, namelast, yearid, cast((h - h2b - h3b - hr + (2 * h2b) + (3 * h3b) + (4 * hr)) AS FLOAT) / (AB) AS slg
  FROM batting as BTable INNER JOIN master as MTable ON Btable.playerid = MTable.playerid
  WHERE BTable.AB > 50
  ORDER BY slg DESC, yearid, MTable.playerid
  LIMIT 10
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
  SELECT MTable.playerid, namefirst, namelast, cast(sum(h) - sum(h2b) - sum(h3b) - sum(hr) + (2 * sum(h2b)) + (3 * sum(h3b)) + (4 * sum(hr)) AS FLOAT) / (sum(AB)) AS lslg
  FROM batting as BTable INNER JOIN master as MTable ON Btable.playerid = MTable.playerid
  GROUP BY MTable.playerid
  HAVING sum(AB) > 50
  ORDER BY lslg DESC, MTable.playerid ASC
  LIMIT 10
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg)
AS
  SELECT namefirst, namelast, cast(sum(h) - sum(h2b) - sum(h3b) - sum(hr) + (2 * sum(h2b)) + (3 * sum(h3b)) + (4 * sum(hr)) AS FLOAT) / (sum(AB)) AS lslg
  FROM batting as BTable INNER JOIN master as MTable ON Btable.playerid = MTable.playerid
  GROUP BY MTable.playerid
  HAVING sum(AB) > 50 AND cast(sum(h) - sum(h2b) - sum(h3b) - sum(hr) + (2 * sum(h2b)) + (3 * sum(h3b)) + (4 * sum(hr)) AS FLOAT) / (sum(AB)) > 
  (SELECT cast(sum(h) - sum(h2b) - sum(h3b) - sum(hr) + (2 * sum(h2b)) + (3 * sum(h3b)) + (4 * sum(hr)) AS FLOAT) / (sum(AB)) FROM batting WHERE batting.playerid = 'mayswi01')
  ORDER By lslg DESC, MTable.playerid ASC
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg, stddev)
AS
  SELECT yearid, min(salary) AS min, max(salary) AS max, avg(salary) AS avg, stddev(salary) AS stddev
  FROM salaries
  GROUP BY yearid
  ORDER BY yearid
;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
AS
  WITH minSalary AS (SELECT min(salary) as mini from salaries where yearid = 2016),
       maxSalary AS (SELECT max(salary) as maxi from salaries where yearid = 2016)

  SELECT bin-1, minSalary.mini + (bin-1) * ((maxSalary.maxi - minSalary.mini) / 10), minSalary.mini + bin * ((maxSalary.maxi - minSalary.mini) / 10), count
  FROM (SELECT width_bucket(salary, minSalary.mini, maxSalary.maxi + 1, 10) AS bin, count(*) AS count
       	FROM salaries, minSalary, maxSalary
  	WHERE yearid = 2016
  	GROUP BY bin
  	ORDER BY bin) AS tbl, minSalary, maxSalary
  ORDER BY bin
;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
  WITH tbl AS (SELECT yearid, min(salary) AS minSal, max(salary) AS maxSal, avg(salary) AS avgSal FROM salaries GROUP BY yearid ORDER by yearid)
  
  SELECT * FROM
  	   (SELECT yearid, minSal - LAG(minSal,1) OVER (ORDER BY yearid) AS mindiff,
  	 	   maxSal - LAG(maxSal,1) OVER (ORDER BY yearid) AS maxdiff,
		   avgSal - LAG(avgSal,1) OVER (ORDER BY yearid) AS avgdiff
		   FROM tbl) AS newQ
 	   WHERE mindiff IS NOT NULL AND maxdiff IS NOT NULL AND avgdiff IS NOT NULL
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
  SELECT MTable.playerid, namefirst, namelast, STable.salary, STable.yearid
  FROM (SELECT playerid, salary 
       FROM salaries
       WHERE salary = (SELECT max(salary) from salaries WHERE yearid = 2000) AND yearid = 2000) AS subq1, salaries AS STable, master as MTable
       WHERE subq1.playerid = STable.playerid AND MTable.playerid = STable.playerid AND yearid = 2000
  
  UNION

  SELECT MTable.playerid, namefirst, namelast, STable.salary, STable.yearid
  FROM (SELECT playerid, salary 
       FROM salaries 
       WHERE salary = (SELECT max(salary) from salaries WHERE yearid = 2001) AND yearid = 2001) AS subq1, salaries AS STable, master as MTable
       WHERE subq1.playerid = STable.playerid AND MTable.playerid = STable.playerid AND yearid = 2001
;
-- Question 4v
CREATE VIEW q4v(team, diffAvg) AS
  SELECT tbl.teamid AS team, max(salary) - min(salary) AS diffavg FROM
  (SELECT allstarfull.playerid, allstarfull.teamid, salaries.salary FROM allstarfull INNER JOIN salaries ON allstarfull.playerid = salaries.playerId AND 
  	 		       			   		   		    	       		   allstarfull.yearid = 2016 AND 
													   salaries.yearid = 2016) AS tbl
  GROUP BY tbl.teamid
;

