import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path

BASE_URL = "https://www.winsipedia.com"
DATA_DIR = Path("dev-local/winsipedia_data")
DATA_DIR.mkdir(exist_ok=True)


# Shared HTTP GET with rate limiting
def http_get(url: str, delay: float = 1.0) -> str:
    """Fetch a URL with retry and basic rate limiting."""
    time.sleep(delay)
    for _ in range(3):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            return r.text
        except requests.RequestException:
            time.sleep(3)
    raise RuntimeError(f"Failed to fetch {url}")


def scrape_winsipedia_teams() -> pd.DataFrame:
    """Scrape team names, nicknames, and URLs from the Teams page."""
    html = http_get(f"{BASE_URL}/team")
    soup = BeautifulSoup(html, "html.parser")

    teams = []
    for div in soup.find_all("div", class_=lambda c: c and "min-w-" in c):
        a = div.find("a", href=True)
        if not a:
            continue
        name_div = div.find("div", class_="text-winsi-blue")
        nick_div = div.find("div", class_="text-muted-foreground")
        if not name_div:
            continue
        team_name = name_div.get_text(strip=True)
        nickname = nick_div.get_text(strip=True) if nick_div else ""
        url = BASE_URL + a["href"]
        teams.append({"team_name": team_name, "nickname": nickname, "url": url})

    df = pd.DataFrame(teams)
    df.to_csv(DATA_DIR / "teams.csv", index=False)
    return df


def scrape_winsipedia_games(school: str) -> pd.DataFrame:
    """Scrape Winsipedia game history for a given school name."""
    html = http_get(f"{BASE_URL}/games/{school.lower().replace(' ', '-')}")
    soup = BeautifulSoup(html, "html.parser")

    rows = []
    for row in soup.select("div[id^='season-'] table tbody tr"):
        cols = [c.get_text(strip=True) for c in row.find_all("td")]
        if len(cols) >= 6:
            result, date, opponent, score, location, notes = cols[:6]
            rows.append(
                {
                    "school": school,
                    "result": result,
                    "date": date,
                    "opponent": opponent,
                    "score": score,
                    "location": location,
                    "notes": notes,
                }
            )

    return pd.DataFrame(rows)


def main():
    teams_df = scrape_winsipedia_teams()

    for _, row in teams_df.iterrows():
        team_name = row["team_name"]
        safe_name = team_name.lower().replace(" ", "_")
        csv_path = DATA_DIR / f"{safe_name}.csv"
        if csv_path.exists():
            continue
        print(f"Fetching {team_name}...")
        try:
            df = scrape_winsipedia_games(team_name)
            if df.empty:
                raise ValueError("empty result set failed")
            df.to_csv(csv_path, index=False)
            print(f"Saved {csv_path}")
        except Exception as e:
            print(f"Failed {team_name}: {e}")


if __name__ == "__main__":
    main()
