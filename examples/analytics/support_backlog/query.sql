SELECT team,
       SUM(CASE WHEN closed_day = 0 THEN 1 ELSE 0 END) AS open_tickets,
       SUM(CASE WHEN closed_day = 0 AND 10 - opened_day >= 5
                THEN 1 ELSE 0 END) AS overdue_open,
       SUM(CASE WHEN closed_day > 0 THEN 1 ELSE 0 END) AS resolved
FROM events
GROUP BY team
ORDER BY team;
