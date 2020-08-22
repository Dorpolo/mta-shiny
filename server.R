# Required Packages
{
  if (!require("shinydashboard")) install.packages("shinydashboard")
  if (!require("jpeg")) install.packages("jpeg")
  if (!require("RPostgres")) install.packages("shinydashboard")
  if (!require("formattable")) install.packages("formattable")
  if (!require("plotly")) install.packages("plotly")
  if (!require("purrr")) install.packages("purrr")
  if (!require('ggcharts')) install.packages("ggcharts") 
  
  # data prep
  library(tidyr)
  library(dplyr)
  library(reshape2)
  library(rlang)
  library(purrr)
  library(lubridate)
  library(glue)
  library(splitstackshape)
  library(devtools)
  library(patchwork)
  # visualizations
  library(ggplot2)
  library(ggcharts)
  library(plotly)
  library(formattable)
  # db connections
  library(RPostgreSQL)
  library(RPostgres)
  # shiny
  library(shiny)
  library(shinyWidgets)
  library(shinythemes)
  library(shinydashboard)
}

# UI elements config
{
  cols = list(yellow='#ffeaa7',blue='#74b9ff',dark_yellow='#fdcb6e',dark_blue='#0984e3')  
  params <- list(font = 'sans',fontface = 'plain')
}

# DB connection
mta_con <- dbConnect(Postgres(),
                     user     = "fzgxltqkgmaklf",
                     password = "6ad610f8f95f1f570ad6c846b68e74f0d692386a8e43d2fce5976f1718e2b779",
                     dbname   = "d5m2p6kka0vf8d",
                     port     = "5432",
                     host     = "ec2-184-73-232-93.compute-1.amazonaws.com",
                     sslmode = 'require')

# DB calls - fetch data
{
  
  games  = dbGetQuery(mta_con,
                      "SELECT *
                       FROM mta_games
                       WHERE league = 'League'")
  
  events = dbGetQuery(mta_con,
                      "SELECT e.event_id,
                              e.date,
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
                       FROM mta_events e
                       INNER JOIN mta_games g ON (g.game_id = e.game_id)
                       INNER JOIN mta_player_con p ON (p.game_id = e.game_id AND p.player_name = e.player_name)
                       WHERE g.league = 'League'
                       ORDER BY e.date, e.minute")
  
  players  = dbGetQuery(mta_con,
                       "SELECT p.*,
                               g.round,
                               g.league,
                               g.season,
                               g.game_result,
                               g.location,
                               g.opponent,
                               g.coach,
                               e.event_id
                        FROM mta_player_con p
                        INNER JOIN mta_games g ON (g.game_id = p.game_id)
                        LEFT JOIN mta_events e ON (g.game_id = e.game_id AND p.player_name = e.player_name)
                        WHERE g.league = 'League'")

  # fetch current league round
  current_season <- dbGetQuery(mta_con,"SELECT season,
                                               max(round) as value
                                        FROM mta_games
                                        WHERE season IN (
                                              SELECT max(season) as season
                                              FROM mta_games
                                              WHERE round is not null
                                              AND league = 'League'
                                          )
                                        GROUP BY 1;")
  # fetch coach nick names
  people <- dbGetQuery(mta_con,"SELECT * 
                                FROM people")
}


function(input, output, session) {

  # reactive datasets
  # games dataset
  dataInput <- reactive({
    games %>%
      filter(season %in% input$season_id,
             round <= input$round_id,
             coach %in% input$coach_id,
             opponent %in% input$opponent_id,
             location %in% input$location_id)
  })
  
  # visual notes
  please_note <- reactive({paste0('League games, rounds 1-',input$round_id) })
  
  # players dataset
  dataPlayers <- reactive({
    players %>%
      filter(season %in% input$season_id,
             round <= input$round_id,
             coach %in% input$coach_id,
             opponent %in% input$opponent_id,
             location %in% input$location_id)
  })
  
  # goals dataset
  u_goals_data <- reactive({ 
    events %>% 
      filter(season %in% input$season_id,
             round <= input$round_id,
             coach %in% input$coach_id,
             opponent %in% input$opponent_id,
             location %in% input$location_id)
  })
  
  box_col <- 'aqua'
  
  ### FIRST TAB - start ###
  
  # Data points info 
  # I. current season (round)
  output$season_round <- renderValueBox({

    mta_stats_box <- games %>% 
      filter(season == current_season$season) %>%
      mutate(points = case_when(game_result == 'W' ~ 3,game_result == 'L' ~ 0,TRUE ~ 1)) %>%
      summarise(goal_scored = sum(mta_score),
                goal_conced = sum(opponent_score),
                league_points = sum(points),
                pct = sum(points)/(n()*3),
                pct_label = paste0(round(pct*100,1),'%'),
                max_round = max(round))
    
    valueBox(   
      value = tags$p(paste0(current_season$season,' (',mta_stats_box$max_round,')'), style = "font-size: 80%;"),
      subtitle = 'Season (Round)', color = box_col
    )
    
  })

  # II. League Points (Success Rate)
  output$points_rate <- renderValueBox({

    mta_stats_box <- games %>% 
      filter(season == current_season$season) %>%
      mutate(points = case_when(game_result == 'W' ~ 3,game_result == 'L' ~ 0,TRUE ~ 1)) %>%
      summarise(goal_scored = sum(mta_score),
                goal_conced = sum(opponent_score),
                league_points = sum(points),
                pct = sum(points)/(n()*3),
                pct_label = paste0(round(pct*100,1),'%'),
                max_round = max(round))
    
    valueBox(
      value = tags$p(paste0(mta_stats_box$league_points,' (',mta_stats_box$pct_label,')'), style = "font-size: 80%;"),
      subtitle = 'Points (Success Rate)',
      icon = icon("calendar fa-1x"), color = box_col
    )

  })
  
  # III. Goal Scored - Conceded
  output$goal_diff <- renderValueBox({
    
    mta_stats_box <- games %>% filter(season == current_season$season) %>%
      mutate(points = case_when(game_result == 'W' ~ 3,game_result == 'L' ~ 0,TRUE ~ 1)) %>%
      summarise(goal_scored = sum(mta_score),
                goal_conced = sum(opponent_score),
                league_points = sum(points),
                pct = sum(points)/(n()*3),
                pct_label = paste0(round(pct*100,1),'%'),
                max_round = max(round))
    
    valueBox(
      value = tags$p(paste0(mta_stats_box$goal_scored,' - ',mta_stats_box$goal_conced), style = "font-size: 80%;"),
      subtitle = "Goal Diff",
      icon = icon("futbol fa-1x"), color = box_col
    )
  })
  
  # season points
  output$seasos_points <- renderPlot({
    
    # data prep
    gg_1 <- dataInput() %>% 
      mutate(points = case_when(game_result == 'W' ~ 3,game_result == 'L' ~ 0,TRUE ~ 1)) %>%
      group_by(season) %>% 
      summarise(points = sum(points),
                goal_scored = sum(mta_score),
                goal_conceeded = sum(opponent_score),
                pct_of_success = sum(points)*100/(n()*3)) %>%
      melt(id = 'season')
    
    # only goals
    gg_1_goals <- gg_1 %>% 
      filter(variable %in% c('goal_scored','goal_conceeded')) %>%
      mutate(variable = factor(variable,levels = c('goal_scored','goal_conceeded'),
                               labels = c('Scored','Conceded')))
 
    ### Goals by Season
    ggplot() + 
      geom_bar(data = gg_1_goals,
               map = aes(x=season,y=value,fill=variable),
               stat='identity',position = position_dodge2(0.9)) + 
      labs(x = '',
           y = '',
           fill = 'Goal',
           subtitle = please_note()) + 
      geom_text(data=gg_1_goals,aes(x=season,y=value,fill=variable,label = value),
                position = position_dodge2(0.9),
                vjust=-0.3,
                family = params$font,
                face = params$fontface,
                size=4,color = 'white') + 
      scale_fill_manual(values = c(cols$yellow,cols$dark_blue)) + 
      guides(fill = guide_legend(nrow=1)) + theme_ng() +
      theme(legend.position = 'top') 

  })
  
  # season success rate
  output$season_success_rate <- renderPlot({
    
    gg_1 <- dataInput() %>% 
      mutate(points = case_when(game_result == 'W' ~ 3,game_result == 'L' ~ 0,TRUE ~ 1)) %>%
      group_by(season) %>% 
      summarise(points = sum(points),goal_scored = sum(mta_score),
                goal_conceeded = sum(opponent_score),
                pct_of_success = sum(points)*100/(n()*3)) %>%
      melt(id = 'season')
    
    # setting the ratio between geom_bar to the geom_line hight
    ratio_factor <- (max(gg_1$value[gg_1$variable == 'pct_of_success'])/max(gg_1$value[gg_1$variable == 'points'])) - 0.2
    
    gg_1_points <- gg_1 %>% filter(variable %in% c('points','pct_of_success'))%>%
      mutate(variable = factor(variable,levels = c('points','pct_of_success'),
                               labels = c('Points','Success rate')))
    
    df_phase_1 = gg_1_points %>% filter(variable == 'Points')
    df_phase_2 = gg_1_points %>% filter(variable == 'Success rate')
    
    ggplot() + 
      geom_bar(data = df_phase_1,
               map = aes(x=season,y=value,fill=variable),
               stat='identity',
               position = position_dodge2(0.9)) + 
      geom_line(data = df_phase_2,
                map = aes(x=season,y=value/ratio_factor,fill=variable),
                group=1,
                col = cols$dark_blue) + 
      geom_point(data = df_phase_2,
                map = aes(x=season,y=value/ratio_factor,fill=variable),
                group=1,
                col =  cols$dark_yellow) + 
      labs(x = '',
           y = '',
           fill = '',
           subtitle = please_note()) + 
      geom_text(data = df_phase_1,
                aes(x=season,y=value,fill=variable,label = value),
                position = position_stack(vjust = 0.5),vjust=-0.4,
                family = params$font,
                face = params$fontface,
                size=4,col = cols$blue) + 
      geom_text(data = df_phase_2,
                aes(x=season,y=value/ratio_factor,fill=variable,label = paste0(round(value,1),'%')),
                position = position_dodge2(0.9),vjust=-0.3,
                family = params$font,face = params$fontface,size=4,col = 'white') + theme_ng() +
      theme(legend.position = 'top') +
            scale_fill_manual(values = c(cols$yellow,cols$dark_blue)) + 
      guides(fill = guide_legend(nrow=1))
    
  })
  
  # season game result distribution
  output$seasos_count <- renderPlot({
    
    gg_2 <- dataInput() %>% 
      group_by(season,game_result,location) %>% 
      summarise(count = n()) %>% group_by(season,location) %>%
      mutate(pct = round(count*100/sum(count)),
             pct_label = paste0(pct,'%')) %>% ungroup() %>%
      mutate(game_result = factor(game_result,levels = c('W','D','L')),
             location = factor(location,levels = c('Home','Away')))
    
    ggplot(data = gg_2,
           map = aes(x = season,y=count,fill=game_result)) + 
      geom_bar(stat = 'identity',
               position=position_fill(0.5)) + 
      facet_grid(~location) +
      scale_y_continuous(labels = scales::percent) + theme_ng() +
      theme(legend.position = 'top')  + 
      geom_text(aes(label = paste0(pct_label,'\n(',count,')')),
                position = position_fill(0.5),
                family = params$font,
                face = params$fontface,
                axis.text.x = element_text(size=8),
                size=2) + 
      scale_fill_manual(values = c(cols$yellow,cols$blue,'#fab1a0'))  + 
      labs(x = 'Season',y = '',fill = '',
           subtitle = please_note())
    
  })
  
  # season coach analysis
  output$season_coach <- renderPlot({
    
    gg_coach_pre <- games %>% 
      mutate(points = case_when(game_result == 'W' ~ 3,game_result == 'L' ~ 0,TRUE ~ 1)) %>%
      group_by(coach) %>% 
      summarise(points = sum(points),
                goal_scored = sum(mta_score),
                goal_conceeded = sum(opponent_score),
                pct_of_success = sum(points)*100/(n()*3),
                games = n()) %>% melt(id = 'coach') 
    
    gg_coach <-
      gg_coach_pre %>%
      left_join(people,by=c('coach'='name')) %>%
      select(coach = nick,variable,value)
    
    relevant_coaches <- gg_coach %>% filter(variable == 'points',value>3) %>% distinct(coach)
    
    gg_coach <- gg_coach %>% filter(coach %in% relevant_coaches$coach)
    
    arrange_coach <- (gg_coach %>% filter(variable == 'pct_of_success') %>% arrange(value))$coach
    
    gg_coach$coach <- factor(gg_coach$coach,levels = arrange_coach)
    
    max_val = max(gg_coach$value)
    
    gg_coach <- gg_coach %>%
      mutate(variable = factor(variable,
                               levels = c('games','points','goal_scored','goal_conceeded','pct_of_success'),
                               labels = c('Games','Points','Goal Scored','Goal Conceded','Success Rate')))
    
    coach_int = gg_coach %>% filter(variable != 'Success Rate')
    coach_pct = gg_coach %>% filter(variable == 'Success Rate')
    
    ##FFBE28
    ggplot() + 
      geom_bar(coach_int,
               map = aes(x=coach,y=value,fill=variable),
               stat='identity',
               position = position_dodge2(0.9)) + 
      geom_point(coach_pct,
                 map = aes(x=coach,y=value*1.5,fill=variable),
                 col='white',
                 group=1,
                 size=1) + 
      labs(x = '',
           y = '',
           fill = '',
           subtitle = 'League games, all time') + 
      geom_text(coach_int,
                map = aes(x=coach,y=value,fill=variable,label = value),
                position = position_dodge2(0.9),
                hjust=-0.3,
                family = params$font,size=3,col='white') + 
      geom_text(coach_pct,
                map = aes(x=coach,y=value*1.5,label = paste0(round(value,1),'%')),
                hjust = -0.5,
                vjust = -0.5,
                position = position_dodge2(0.9),
                family = params$font,size=2.5,
                col = 'white',
                fill='white') + 
      theme(legend.position='top') + 
      theme_ng() +
      theme(legend.position = 'top',
            legend.justification='left',
            legend.direction='horizontal') + 
      scale_fill_manual(values = rev(c('white',cols$yellow,cols$blue,'#fab1a0','#ABABAB'))) + 
      guides(fill = guide_legend(nrow=2),col = F,points = F) +
      coord_flip()
  })
  
  # all games table
  output$table <- formattable::renderFormattable({
  
    games_table <- dataInput() %>% 
      left_join(people,by = c('coach'='name')) %>%
      select(season,date,stadium,location,opponent,mta_score,opponent_score,round,nick,game_result) %>%
      mutate(home = ifelse(location == 'Away',opponent,'MTA'),
             away = ifelse(location == 'Away','MTA',opponent),
             result = ifelse(location == 'Away',paste0(opponent_score,'-',mta_score),paste0(mta_score,'-',opponent_score)),
             date = as.Date(date)) %>%
      select(Season=season,Round=round,Date=date,Stadium=stadium,Home=home,Away=away,Result = result,Coach=nick,`Winner` = game_result) %>%
      arrange(desc(Season),desc(Round)) %>%
      mutate(Date = substring(Date,3,10))
    
    improvement_formatter <- formatter("span", 
                                       style = x ~ formattable::style("font-size:8px;",color = ifelse(x == 'W', "green",ifelse(x == 'D',"orange",'red'))), 
                                       x ~ icontext(ifelse(x == 'W', "star",ifelse(x == 'D',"",'')), x))
    
    formattable(games_table,
                align = rep('c',ncol(games_table)),
                list(`Season` = formatter('span'),
                     `Winner` = improvement_formatter,
                     `Home` =formatter('span',style = x ~ ifelse(x == 'MTA',"font-size:8px;","font-size:8px;")),
                     `Away` =formatter('span',style = x ~ ifelse(x == 'MTA',"font-size:8px;","font-size:8px;")),
                     `Stadium` = formatter('span',style = x ~ formattable::style("font-size:8px;")),
                     `Season` = formatter('span',style = x ~ formattable::style("font-size:8px;")),
                     `Round` = formatter('span',style = x ~ formattable::style("font-size:8px;")),
                     `Date` = formatter('span',style = x ~ formattable::style("font-size:8px;")),
                     `Result` = formatter('span',style = x ~ formattable::style("font-size:8px;")),
                     `Coach` = formatter('span',style = x ~ formattable::style("font-size:8px;")))) 
  })
  
  ### FIRST TAB - end ###
  
  ### SECOND TAB - start ###
  
  # games count (divided to W/D/L and starter/ sub) by player
  output$p_games <- renderPlot({
    
    g_games <- dataPlayers() %>% 
      filter(is_played == T) %>%
      group_by(season,
               player_name,
               game_result,
               game_status) %>% 
      summarise(games = n_distinct(game_id)) %>% 
      ungroup()
    
    games_order <- g_games %>% 
      group_by(player_name) %>% 
      summarise(games = sum(games[season == current_season$season])) %>% 
      arrange(games) 
    
    games_adj <- g_games %>% mutate(player_name = factor(player_name,levels = games_order$player_name),
                                    game_result = factor(game_result,levels = c('L','D','W'),labels = c('Lose','Draw','Win')),
                                    game_status = factor(game_status,levels = c('substitute','opening'),labels = c('Sub','Open'))) 
    
    this_year_players <- games_adj %>% filter(season == current_season$season) %>% distinct(player_name)
    
    ggplot(games_adj %>% 
             filter(player_name %in% this_year_players$player_name,
                    season == current_season$season),
           aes(x=player_name,y=games,fill=game_result,alpha = game_status)) + 
      geom_bar(stat='identity',
               position = position_stack(0.9)) + 
      coord_flip() + 
      theme_ng() +
      theme(legend.position='top', 
            legend.justification='left',
            legend.direction='horizontal') + 
      geom_text(aes(label = games),
                position = position_stack(0.5),
                size=4) + 
      labs(x = 'Minutes',
           y='',
           fill = '',
           alpha = '',
           title = '',
           subtitle = please_note()) + 
      scale_fill_manual(values =  c(cols$blue,cols$yellow,'#fd79a8')) + 
      scale_alpha_discrete(range = c(0.3, 0.9)) + 
      guides(alpha = guide_legend(nrow=2),
             fill = guide_legend(nrow=2))
   
  })
  
  # aggregated minutes by player
  output$p_minutes <- renderPlot({
    
    g_minutes <- dataPlayers() %>%
      group_by(season,player_name) %>% 
      summarise(games = n_distinct(game_id),
                minutes = sum(minutes_played)) %>% 
      ungroup() %>% filter(minutes>0)
    
    # ranking seasons by years
    season_name = sort(unique(g_minutes$season))
    len = length(season_name)
    
    # name location, based on season relavncy
    season_weight = tibble(season = season_name,
                           rank = rank(desc(season_name))) %>%
      mutate(weight = case_when(rank == 1 ~ round(len/3)/len,
                                rank == 2 ~ round(len/4)/len,
                                rank == 3 ~ round(len/6)/len,
                                TRUE ~ ((len - round(len/3) -
                                           round(len/4) - 
                                           round(len/6))/(len - 3))/len
      ))
    
    minutes_order <- g_minutes %>% 
      inner_join(season_weight,by = 'season') %>%
      group_by(player_name) %>% 
      summarise(minutes =  sum(minutes*weight)) %>% 
      arrange(minutes) 
    
    g_minutes$player_name <- factor(g_minutes$player_name,levels = minutes_order$player_name)
    
    this_year_players <- g_minutes %>% 
      filter(season %in% (season_weight %>% filter(rank %in% 1:3))$season,
             minutes > 90) %>% 
      distinct(player_name)
    
    ggplot(g_minutes %>% 
             filter(player_name %in% this_year_players$player_name),
           aes(x=player_name,y=minutes,fill=season)) + 
      geom_bar(stat='identity',
               position = position_dodge2(width = 0.9, preserve = "single")) + 
      coord_flip() + 
      theme_ng() + 
      theme(legend.position='top', 
            legend.justification='left',
            legend.direction='horizontal') + 
      geom_text(aes(label = round(minutes)),
                position = position_dodge2(0.5),hjust=-0.3,size=2.5,color='white') + 
      labs(x = '',
           y = '',
           fill = '',
           subtitle = please_note()) 
  })
  
  # goal scored - active players
  output$p_goals_active <- renderPlot({
    
    g_goals <- dataPlayers() %>% 
      filter(is_played == T) %>%
      group_by(season,player_name) %>% 
      summarise(goals = sum(!is.na(event_id))) %>% 
      ungroup()
    
    season_name = sort(unique(g_goals$season))
    len = length(season_name)
    
    season_weight = tibble(season = season_name,
                           rank = rank(desc(season_name))) %>%
      mutate(weight = case_when(rank == 1 ~ round(len/3)/len,
                                rank == 2 ~ round(len/4)/len,
                                rank == 3 ~ round(len/6)/len,
                                TRUE ~ ((len - round(len/3) -
                                           round(len/4) - 
                                           round(len/6))/(len - 3))/len
      ))
    
    goals_order <- g_goals %>% group_by(player_name) %>%
      summarise(goals = sum(goals[season == current_season$season])) %>%
      arrange(goals)

    goals_adj <- g_goals %>% mutate(player_name = factor(player_name,levels = goals_order$player_name))

    this_year_players <- goals_adj %>% filter(season == current_season$season) %>% distinct(player_name)

    ggplot(goals_adj %>% filter(player_name %in% this_year_players$player_name,goals>0),
           aes(x=player_name,y=goals,fill=season)) +
      geom_bar(stat='identity',
               position = position_dodge2(width = 0.9, preserve = "single")) + 
      coord_flip() + 
      theme_ng() + 
      theme(legend.position='top',
            legend.justification='left',
            legend.direction='horizontal') +
      geom_text(aes(label = ifelse(goals>0,goals,'')),
                position = position_dodge2(0.9),
                size=4,
                hjust=-0.3,
                color='white') +
      labs(x = 'Goal Scored',
           y='',
           fill = '',
           alpha = '',
           subtitle = please_note())

  })
  
  # goal scored - legacy players  
  output$p_goals_legacy <- renderPlot({
    
    g_goals <- dataPlayers() %>% filter(is_played == T) %>%
      group_by(season,player_name) %>% 
      summarise(goals = sum(!is.na(event_id))) %>% 
      ungroup()
    
    season_name = sort(unique(g_goals$season))
    len = length(season_name)
    
    season_weight = tibble(season = season_name,
                           rank = rank(desc(season_name))) %>%
      mutate(weight = case_when(rank == 1 ~ round(len/3)/len,
                                rank == 2 ~ round(len/4)/len,
                                rank == 3 ~ round(len/6)/len,
                                TRUE ~ ((len - round(len/3) -
                                           round(len/4) - 
                                           round(len/6))/(len - 3))/len
      ))
    
    goals_order <- g_goals %>% 
      inner_join(season_weight,by = 'season') %>% 
      group_by(player_name) %>% 
      summarise(goals = sum(goals*weight)) %>% 
      arrange(goals)
    
    goals_adj <- g_goals %>% mutate(player_name = factor(player_name,levels = goals_order$player_name)) 
    
    this_year_players <- goals_adj %>% filter(season == current_season$season) %>% distinct(player_name)
    
    df_goals <- goals_adj %>% filter(goals>0) %>%
      mutate(this_year = ifelse(player_name %in% this_year_players$player_name,T,F))
    
    ggplot(df_goals %>% filter(this_year == F),
           aes(x=player_name,y=goals,fill=season)) + 
      geom_bar(stat='identity',
               position = position_dodge2(width = 0.9, preserve = "single")) + coord_flip() + 
      theme_ng() + 
      theme(legend.position='top', 
            legend.justification='left',
            legend.direction='horizontal') + 
      geom_text(aes(label = ifelse(goals>0,goals,'')),
                position = position_dodge2(0.9),size=4,hjust=-0.3,color='white') + 
      labs(x = 'Goal Scored',
           y='',
           fill = '',
           alpha = '',
           subtitle = please_note()) 
    
    
  })
  
  ### SECOND TAB - end ###
  
  ### THIRD TAB - start ###
  
  # number of unique scorers
  output$goal_rec <- renderPlot({

    u_goals = u_goals_data() %>%
      group_by(season,game_status) %>% 
      distinct(player_name,.keep_all = T) %>%
      summarise(scorers = n_distinct(player_name))

    all = u_goals %>% filter(game_status != 'substitute') %>% group_by(season) %>% summarise(scorers = sum(scorers)) %>% mutate(type = 'Total scorers')
    sub = u_goals %>% filter(game_status == 'substitute') %>% select(season,scorers) %>% mutate(type = 'Scorers as a sub')
    
    u_goals = bind_rows(all,sub)
    
    f <- list(
      family = "Courier New, monospace",
      size = 18,
      color = "#7f7f7f"
    )
    
    ### Goals by Season
    ggplot() +
      geom_bar(data = u_goals,
               map = aes(x=season,y=scorers,fill = type),
               stat='identity',position = position_dodge2(0.9)) + 
      geom_text(data = u_goals,
                aes(label=scorers,x=season,y=scorers,fill = type),
                position = position_dodge2(0.9),
                vjust=-0.1,
                family = params$font,size=4,color = 'white') + 
      scale_fill_manual(values = c(cols$dark_blue,cols$yellow)) + 
      theme_ng() + 
      labs(title = 'Unique scorers',
           x = 'Season',y = 'Players',fill='',
           subtitle = please_note()) +
      theme(legend.position = 'top') 
    
    
  })
  
  # clean sheet - game count
  output$clean_sheet <- renderPlot({
    
    dataInput() %>%
      group_by(season,opponent_score) %>%
      summarise(N = n()) %>% 
      group_by(season) %>% 
      mutate(`%` = round(N*100/sum(N))) %>%
      ggplot(aes(x = season,
                 y = N,
                 fill=as.factor(opponent_score))) + 
      geom_bar(stat = 'identity',
               position = position_dodge2()) + 
      theme_ng() +
      theme(legend.position='top',
            legend.justification='left',
            legend.direction='horizontal') +
      labs(fill = 'Number of goals per game',
           title = 'Match count - goals conceded',
           y = '',
           x = 'Season',
           subtitle = please_note()) + 
      geom_text(aes(label = paste0(N,'\n',`%`,'%')),
                position = position_dodge2(0.9),
                size=2.5,vjust=-0.2,color='white') 
    
  })

  ### THIRD TAB - end ###
  
}
   

   

