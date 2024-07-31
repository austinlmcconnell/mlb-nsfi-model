# MLB 1st Inning Betting Model

Dashboard Link: https://rb.gy/sw0891

Developed a model in Python that determines the probability of outcomes such as no strikeouts or no hits occurring on the top or bottom of the 1st inning in specific baseball games. The idea of this model is to deploy it against various sports betting lines to mine out positive expected value and make long-term profit.

** Statistics Used **
- Pitcher strikeout, walk, single, double, triple, and home run rates
    - Vs. left- and right-handed batters
    - From the beginning of the 2023 season (the first time through the lineup)
    - From the beginning of the 2024 season (all batters faced, weighed 20% higher)
- Batter strikeout, walk, single, double, triple, and home run rates
    - Vs. left- and right-handed pitchers
    - From the beginning of the 2023 season
    - From the beginning of the 2024 season (weighed 20% higher)
- MLB average strikeout, walk, single, double, triple, and home run rates
    - From beginning of the 2024 season
- MLB ballpark impacts on batting rates
    - https://swishanalytics.com/mlb/mlb-park-factors
    - From beginning of 2014 season
    - For both left- and right-handed batters
 
** Resources **
- FanGraphs splits leaderboard (https://www.fangraphs.com/leaders/splits-leaderboards?splitArr=&splitArrPitch=&position=P&autoPt=false&splitTeams=false&statType=player&statgroup=1&startDate=2024-04-01&endDate=2024-11-30&players=&filter=&groupBy=career&sort=1,1&wxTemperature=&wxPressure=&wxAirDensity=&wxElevation=&wxWindSpeed=)
- Swish Analytics MLB park factors (https://swishanalytics.com/mlb/mlb-park-factors)
- RotoWire daily baseball lineups (https://www.rotowire.com/baseball/daily-lineups.php)
- Gaming Today implied betting probability calculator (https://www.gamingtoday.com/tools/implied-probability/)
- NSFI model spreadsheet tracker of 5+ % expected value bets based on betting odds from DraftKings Sportsbook and bet365 (https://docs.google.com/spreadsheets/d/1znP3X5QEaxgM0pGvTp6nzVLTibVZyBv3mav1x7ufvjU/edit?usp=sharing)
