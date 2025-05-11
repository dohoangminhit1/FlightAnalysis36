# FBRef Premier League Match Scraper

## Description

This project scrapes match statistics for English Premier League seasons from [FBRef.com](https://fbref.com) and saves the data into a CSV file. It extracts detailed match information, team and player statistics, and event summaries from season schedules and individual match reports.

## Data Source

- **Website:** [FBRef.com](https://fbref.com)
- **Season Schedule:** Contains links to all match reports for a season, e.g., [2023-2024 Premier League Scores & Fixtures](https://fbref.com/en/comps/9/2023-2024/schedule/2023-2024-Premier-League-Scores-and-Fixtures)
- **Individual Match Reports:** Detailed pages for each match, accessible via links from the season schedule.

**Note:** Use this scraper responsibly, respecting [FBRef.com](https://fbref.com)'s terms of service and robots.txt. Include delays between requests to avoid overloading the server.

## Website Structure

The scraper targets match report pages on [FBRef.com](https://fbref.com), which follow a consistent HTML structure. Key sections include:

- **Title and Overview (`div#content`):** Contains the match title (`h1`) and general information like league and matchweek.
- **Scorebox (`div.scorebox`):** Includes team logos, names, scores, and metadata (date, venue, officials).
- **Lineups (`div.lineup#a`, `div.lineup#b`):** Lists starting and bench players for home and away teams.
- **Team Stats (`div#team_stats`, `div#team_stats_extra`):** Summarizes possession, passing accuracy, shots, saves, cards, fouls, corners, etc.
- **Player Stats (`div#all_player_stats_[TEAM_ID]`):** Details individual player performance, including goals, assists, shots, and cards.
- **Goalkeeper Stats (`div#all_keeper_stats_[TEAM_ID]`):** Covers goalkeeper-specific metrics like saves and shots faced.
- **Match Events (`div#events_wrap`):** Logs key events like goals, cards, and substitutions with timestamps.

The scraper uses `BeautifulSoup` to parse these elements. For a detailed HTML structure, see [htmltree.txt](htmltree.txt) in the repository.

## Requirements

- **Python:** 3.10.13
- **Libraries:**
  - `requests`
  - `beautifulsoup4`
  - `pandas`
  - `lxml`
  - `selenium`
- **Google Chrome Browser:** Required for Selenium with ChromeDriver.
- **ChromeDriver:**
  - Must match your Chrome browser version.
  - **Check Chrome Version:** Navigate to `chrome://settings/help`.
  - **Download ChromeDriver:** Get the matching version from [ChromeDriver Downloads](https://chromedriver.chromium.org/downloads) or [Chrome for Testing](https://googlechromelabs.github.io/chrome-for-testing/).
  - **Installation:**
    1. Place the `chromedriver` executable in a known directory.
    2. Add its directory to your system's PATH or specify the full path in the Selenium script.

## Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```
2. **Create and activate a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
4. **Install ChromeDriver:**
   - Download the correct version for your Chrome browser.
   - Add to PATH or note the file path for use in the script.

## Usage

1. **Open the Jupyter Notebook:**
   - General use: `epl_scraper.ipynb`
   - Season-specific (if available): e.g., `epl_scraper_2017.ipynb`
2. **Set the season URL:**
   - Find the URL for the desired season's schedule on [FBRef.com](https://fbref.com).
   - Example: For 2014-2015, use `https://fbref.com/en/comps/9/2014-2015/schedule/2014-2015-Premier-League-Scores-and-Fixtures`.
   - Update the `season_url` variable in the notebook.
3. **Run all cells in the notebook:**
   - The scraper will:
     1. Fetch match report links from the season schedule.
     2. Extract data from each match report.
     3. Save results to a CSV file (e.g., `fbref_2014_2015_premier_league_stats.csv`).

### Finding Season URLs

- Visit [FBRef.com](https://fbref.com).
- Select "Premier League" from the competitions dropdown.
- Choose the desired season.
- Copy the URL of the "Scores & Fixtures" page.

## Output CSV Fields

The CSV output contains the following fields:

| Field                  | Description                                      |
|------------------------|--------------------------------------------------|
| `match_date`           | Date of the match (e.g., 2014-08-16)            |
| `week`                 | Matchweek number                                |
| `league`               | League name (e.g., Premier League)              |
| `season`               | Season (e.g., 2014-2015)                        |
| `round`                | Round or matchweek                              |
| `venue`                | Match venue                                     |
| `match_report_url`     | URL of the match report                         |
| `source`               | Data source (e.g., FBRef)                       |
| `home_team`            | Home team name                                  |
| `away_team`            | Away team name                                  |
| `home_score`           | Goals scored by home team                       |
| `away_score`           | Goals scored by away team                       |
| `home_lineup`          | Starting players for home team                  |
| `away_lineup`          | Starting players for away team                  |
| `home_bench`           | Bench players for home team                     |
| `away_bench`           | Bench players for away team                     |
| `home_missing`         | Missing players for home team                   |
| `away_missing`         | Missing players for away team                   |
| `possession_home`      | Home team possession percentage (e.g., 59%)     |
| `possession_away`      | Away team possession percentage (e.g., 41%)     |
| `pass_comp_home`       | Home team pass completion percentage (e.g., 79%)|
| `pass_comp_away`       | Away team pass completion percentage (e.g., 70%)|
| `shots_home`           | Total shots by home team                        |
| `shots_away`           | Total shots by away team                        |
| `shots_on_target_home` | Shots on target by home team                    |
| `shots_on_target_away` | Shots on target by away team                    |
| `saves_home`           | Saves by home goalkeeper                        |
| `saves_away`           | Saves by away goalkeeper                        |
| `yellow_cards_home`    | Yellow cards for home team                      |
| `yellow_cards_away`    | Yellow cards for away team                      |
| `red_cards_home`       | Red cards for home team                         |
| `red_cards_away`       | Red cards for away team                         |
| `fouls_home`           | Fouls committed by home team                    |
| `fouls_away`           | Fouls committed by away team                    |
| `corners_home`         | Corners won by home team                        |
| `corners_away`         | Corners won by away team                        |

### Example CSV Row

```
match_date,week,league,season,home_team,away_team,home_score,away_score,possession_home,possession_away,...
2014-08-16,1,Premier League,2014-2015,Liverpool,Southampton,2,1,59%,41%,...
```

## Ethical Considerations

This project is intended for educational or personal use only. Respect [FBRef.com](https://fbref.com)'s terms of service and robots.txt. Do not use for commercial purposes without permission. Implement delays (e.g., 3-5 seconds between requests) to minimize server load.

## Contributing

Contributions are welcome! Please open an issue to discuss changes before submitting a pull request.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.