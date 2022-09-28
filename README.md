# Pollen Mobile Scraper

A simple script to scrape Pollen Mobile data into a Postgres Database. This can be used as a cron script to keep an up-to-date copy for your own custom frontend or scripts.

## Running

This requires Go 1.18+ installed. After you clone the repository, running it is simple:

```
go run main.go
```
Optionally, if you want to also sync certain geographic hexes, you can list them:
```
go run main.go "852a1393fffffff,852a104bfffffff,852a1057fffffff" "85283457fffffff,852830c7fffffff,85283467fffffff"
```

## Schema

The script populates 3 tables: `pollen_flowers`, `pollen_hexes`, and `pollen_rewards`. Each stores representative information from the API, along with canonical IDs to join against tables.


## Hex Groupings

Here are a few H3 Hex Bounding Boxes I've been using:

| Area        | Hex Bounding Box  |
| ------------- |:-------------:|
| New York City      | `"852a1393fffffff,852a104bfffffff,852a1057fffffff,852a1063fffffff,852a100bfffffff,852a106ffffffff,852a13c3fffffff,852a107bfffffff,852a102ffffffff,852a1383fffffff,852a103bfffffff,852a1047fffffff,852a139bfffffff,852a106bfffffff,852a1077fffffff,852a12b7fffffff,852a102bfffffff,852a13d7fffffff,852a138bfffffff,852a1043fffffff,852a1397fffffff,852a104ffffffff,852a1003fffffff,852a1067fffffff,852a100ffffffff,852a12a7fffffff,852a1073fffffff,852a101bfffffff,852a13c7fffffff,852a12b3fffffff,852a13d3fffffff"` |
| San Francisco      | `"85283457fffffff,852830c7fffffff,85283467fffffff,8528346ffffffff,85283403fffffff,852830d7fffffff,85283477fffffff,8528340bfffffff,8528341bfffffff,85283083fffffff,8528342bfffffff,8528308bfffffff,85283093fffffff,8528343bfffffff,8528309bfffffff,85283443fffffff,85283453fffffff,852836a7fffffff,852830c3fffffff,85283463fffffff,852830cbfffffff,8528346bfffffff,852836b7fffffff,852830d3fffffff,85283473fffffff,852830dbfffffff,85283407fffffff,8528347bfffffff,8528340ffffffff,85283417fffffff,8528308ffffffff,85283447fffffff,8528344ffffffff"` |

## Example Queries

Most Profitable Hexes in a Given Area
```
SELECT id, flower_count, bounty_reward, loot_box_reward, daily_reward
FROM pollen_hexes
WHERE suburb = 'Brooklyn'
ORDER BY 3 DESC LIMIT 10;
```

Daily Rewards By Type
```
SELECT date, reward, SUM(PCN)
FROM pollen_rewards
WHERE date >= '2022-09-10'
GROUP BY 1,2 ORDER BY 1 DESC, 2 ASC LIMIT 10
```

Top Flower Locations
```
SELECT COALESCE(city, town, county, suburb) AS location, COUNT(1)
FROM pollen_flowers
GROUP BY 1 ORDER BY 2 DESC LIMIT 10
```
