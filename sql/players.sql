SELECT p.*,
       g.round,
       g.league,
       g.season,
       g.game_result,
       g.location,
       g.opponent,
       g.coach,
      e.event_type
FROM mta.players p
INNER JOIN mta.games g ON (g.game_id = p.game_id)
LEFT JOIN mta.events e ON (
                           g.game_id = e.game_id AND 
                           p.player_name = e.player_name AND 
                           e.event_type = 'goal_scored'
                           )
WHERE g.league = 'League'