# Companies House Leads Tracker

This repository represents a solution to a programming take home task I have completed during an inverview process for a position at a food delivery company in Amsterdam, the Netherlands. More specifically, this is my second attempt to complete it using [AsyncIO](https://docs.python.org/3/library/asyncio.html), first one is [here](https://github.com/Lifeissimple-zxc/test_exercise_companies_house). Apart from concurrency, I have also used this development to try implementing logging to sqlite and [Discord](https://discord.com/).<br>

---

## Motivation (problem statement)

Sales team wants to build out a marketing campaign that targets newly opened restaurants. Currently, there is no easy way for them to get this information. They have heard it is possible to get this information from the [Companies House
website](https://developer-specs.company-information.service.gov.uk/). They want an automated solution that gets relevant data about newly incorporated restaurants
and shares it with them on a **Google sheet**. <br>The following data points are required for a lead to participate in the campaign:
1. Company Name
2. Company Number
3. Registered Office Address
4. Date of Creation
5. Company Type
6. SIC Codes
7. Company officer names<br>

---

## Implementation & Logic Overview

The main module of the project is *tracker.py*, this file is meant to be a scheduled job executed every *X* minutes, hours or days (depends on preferences).

The code relies on the following endpoints provided by the API:
+ [Advanced Company Search](https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference/search/advanced-company-search) for locating newly created companies.

+ [Company Officers](https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference/officers/list) for extracting officers data on the recently created companies. We only go to this endpoint after we get results from the advanced search.

High-level logic can be described with the following chart:<br>![High-level logic](https://github.com/Lifeissimple-zxc/random_stuff/blob/main/Main%20Logic.png)<br>
Request processing can be described with the following sequence of steps:<br>![Request processing](https://www.dropbox.com/s/soqjo275e4eagmi/Request%20processing.png?dl=0)<br>

### Logging

All logs are saved to a local sqlite db. In addition, **WARNING** and above log records are sent to discord.

---

