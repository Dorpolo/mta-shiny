SELECT e.date,
       e.game_id,
       e.player_name,
       e.minute,
       g.round,
       g.league,
       g.season,
       g.game_result,
       g.location,
       g.opponent,
       g.coach,
       p.minutes_played,
       p.game_status
FROM mta.events e
INNER JOIN mta.games g ON (g.game_id = e.game_id)
INNER JOIN mta.players p ON (p.game_id = e.game_id AND p.player_name = e.player_name)
WHERE g.league = 'League'
AND e.event_type = 'goal_scored'
ORDER BY e.date, e.minute