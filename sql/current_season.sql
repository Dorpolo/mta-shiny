SELECT season,
       max(round) as value
FROM mta.games
WHERE season IN (
      SELECT max(season) as season
      FROM mta.games
      WHERE round is not null
      AND league = 'League'
  )
AND league = 'League'
GROUP BY 1;