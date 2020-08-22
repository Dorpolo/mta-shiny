# required packages
{
  if (!require("shinydashboard")) install.packages("shinydashboard")
  if (!require("shinyWidgets")) install.packages("shinyWidgets")
  if (!require("plotly")) install.packages("plotly")
  
  library(shinyWidgets)
  library(shinydashboard)
  library(shiny)
  library(RPostgres)
  library(RPostgreSQL)
  library(formattable)
  library(plotly)
}

# ui configurations
{
  status_style <- 'primary'
}

# DB connection
{
  mta_con <- dbConnect(Postgres(),
                       user     = "fzgxltqkgmaklf",
                       password = "6ad610f8f95f1f570ad6c846b68e74f0d692386a8e43d2fce5976f1718e2b779",
                       dbname   = "d5m2p6kka0vf8d",
                       port     = "5432",
                       host     = "ec2-184-73-232-93.compute-1.amazonaws.com",
                       sslmode = 'require')
}

# DB calls
{
  # fetch all time league games
  games  = dbGetQuery(mta_con,"SELECT *
                               FROM mta_games
                               WHERE league = 'League'")
  
  # fetch current league round
  current_round <- dbGetQuery(mta_con, "SELECT season,
                                             max(round) as value
                                      FROM mta_games
                                      WHERE season IN (
                                          SELECT max(season) as season
                                          FROM mta_games
                                          WHERE round is not null
                                          AND league = 'League'
                                      )
                                      GROUP BY 1;")
}


dashboardPage(skin = "yellow",
  # header            
  dashboardHeader(title = "MTA"),
  # side bar filters
  dashboardSidebar(
        # tab configuration
        sidebarMenu(
                    menuItem("Games", tabName = "team_view"),
                    menuItem("Players", tabName = "player_view"),
                    menuItem("Records", tabName = "rec")
                    ),
        # filter 1: league round
        # type: slider
                    sliderInput('round_id',
                                'Round',
                                min(games$round),
                                max(games$round),
                                current_round$value,
                                1 ,
                                '100%'),
        # filter 2: Home/ Away
        # type: picker
                    pickerInput('location_id',
                                'Location',
                                choices=unique(games$location),
                                selected = unique(games$location),
                                options = list(`actions-box` = TRUE),
                                multiple = T),
        # filter 2: Season
        # type: picker
                    pickerInput('season_id',
                                'Season',
                                choices=unique(games$season),
                                selected = unique(games$season),
                                options = list(`actions-box` = TRUE),
                                multiple = T),
        # filter 2: Coach
        # type: picker
                    pickerInput('coach_id',
                                'Coach',
                                choices=unique(games$coach),
                                selected = unique(games$coach),
                                options = list(`actions-box` = TRUE),
                                multiple = T),
        # filter 2: Opponent
        # type: picker
                    pickerInput('opponent_id',
                                'Opponent',
                                choices=unique(games$opponent),
                                selected = unique(games$opponent),
                                options = list(`actions-box` = TRUE),
                                multiple = T),
                    width = '200px'
  ),
  # BODY
  dashboardBody(
    # generate tab items
    tabItems(
      # first tab - team
      tabItem("team_view",
      # this season general info
    fluidRow(
      valueBoxOutput("season_round", width = 4),
      valueBoxOutput("points_rate", width = 4),
      valueBoxOutput("goal_diff", width = 4)
    ),
     # first row
      fluidRow(
        # success rate
        box(width = 6,
            solidHeader = TRUE,
            status = "primary",
            collapsible = TRUE,
            plotOutput("season_success_rate",inline=F, width='100%', height=300),
            style = 'display:block;width:100%;',
            title = "Success"),
        # season points
        box(width = 6,
            solidHeader = TRUE,
            status = 'primary',
            collapsible = TRUE,
            plotOutput("seasos_points", inline=F, width='100%', height=300),
            style = 'display:block;width:100%;',
            title = "Goals")
      ),
    # second row
      fluidRow(
        # match result distribution
        box(width = 12,
            solidHeader = TRUE,
            status = 'primary',
            collapsible = TRUE,
            plotOutput("seasos_count",width='100%', height=300),
            style = 'display:block;width:100%;',
            title = "Match Results")
        ),
       # third row
    fluidRow(
       # first column
      column(width = 6,
      # coach analysis       
      box(width = '100%',
          title = "Coaches",
          solidHeader = TRUE,
          status = 'primary',
          collapsible = TRUE,
          plotOutput("season_coach",width='100%', height=500),
          style = 'display:block;width:100%;')
      ), # closing column
      # second column
     column(width = 6,
    # all games table     
    box(width = '100%',
        title = "Raw Data",
        solidHeader = TRUE,
        status = 'primary',
        collapsible = TRUE,
        div(style = "height:500px; overflow-y: scroll; overflow-x: scroll;",formattable::formattableOutput('table')),
        style = 'display:block;width:100%;')
    ) # closing column
    ) 
),  # closing tab
    # second tab - players  
    tabItem("player_view",
      # first row
      fluidRow(
        # first column
        column(width = 6,
            # game count
            box(width = '100%',
                title = "S:19-20 - Games Played",
                solidHeader = TRUE,
                status = 'primary',
                collapsible = TRUE,
                plotOutput("p_games",width='100%', height=500),
                style = 'display:block;width:100%;')
            ),
      # second column
      column(width = 6,
             # minutes analysis
          box(width = '100%',
              title = "Minutes Played by Season",
              solidHeader = TRUE,
              status = 'primary',
              collapsible = TRUE,
              plotOutput("p_minutes",width='100%', height=500),
              style = 'display:block;width:100%;')
          ) # closing column
      ), # closing row
       # second row
       fluidRow(
         # first column
         column(width = 6,
           # goals scored - current players
           box(width = '100%',
               title = "Goal Scored - Active Players",
               solidHeader = TRUE,
               status = 'primary',
               collapsible = TRUE,
               plotOutput("p_goals_active",width='100%', height=500),
               style = 'display:block;width:100%;')
           ), # closing column
         # second column
        column(width = 6,
         # goal scored - legacy players
         box(width = '100%',
             title = "Goal Scored - Legacy Players",
             solidHeader = TRUE,
             status = 'primary',
             collapsible = TRUE,
             plotOutput("p_goals_legacy",width='100%', height=500),
             style = 'display:block;width:100%;')
         ) # closing column
        ) #closing row
      ), # closing tab

    # third tab - records
    tabItem("rec",
        # first row
        fluidRow(
          # first column
          column(width = 12,
                 # number of unique scorers
                box(width = '100%',
                    title = "Unique Scorers",
                    solidHeader = TRUE,
                    status = 'primary',
                    collapsible = TRUE,
                    plotOutput("goal_rec",width='100%', height=350),
                    style = 'display:block;width:100%;')
                )  # closing column
          ), # closing row
        # second row
        fluidRow(
          # first column
          column(width = 12,
                 # clean sheet count
                  box(width = '100%',
                      title = "Clean Sheet",
                      solidHeader = TRUE,
                      status = 'primary',
                      collapsible = TRUE,
                      plotOutput("clean_sheet",width='100%', height=420),
                      style = 'display:block;width:100%;')
                 ) # closing column
          ) # closing row
        ) # closing tab
  ),
  # choosing a title
  title = "Real Maccabi"
) # closing body
) # closing all


