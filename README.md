# SQL-Processes-Amalgamation-
All the custom SQL stuff I've done for my job @ DSI with sensitive info removed. 


Hired in as "Pricing Analyst" but my role has grown into so much more.
First role was dealing with price files from vendors, then was assigned projects that included setup (and maintenance) for a B2B website (cull duplicates in DB, fill in missing info, etc), automate processes (typically to increase sales productivity), rebates, just solve problems in general in relation to the DB, inventory, orders, etc.


The DB was setup (I believe) by the creators of our ERP system, Prophet 21 by Epicor. 
A lot of my code is aimed at avoiding using their app or web application to upload imports, as it took a significant amount of time to prep and create those, and then for P21 to actually process them. Quite a bit of my solution was just uploading data to my own temp table I created and making transactions with that table.
SQLAlchemy and Pandas were apart of my default boilerplate and ETL. 
Sensitive info replaced in SQLALCH engine query, so visually those lines are going to look fairly generic. 
Some scripts are similar to others, and as time goes on I ended up refactoring some of my most used code.



Here's code that I typically use at my job to complete various functions:

Update our Distributor pricing from core vendors. Multiple price increases can happen at one time. 

Get data for our consignment locations and generate strategic pricing that falls within our contract promises while still being profitable in a volatile market.

Generate a list of records that any person has changed or made for whatever reason. (Typically used to roll back updates if we received bad data or identify users who are struggling or misunderstanding their roles or data). 

Identify top (or recent) items sold by a vendor to hand off to salespeople to negotiate Special Pricing Agreements.

Connect customer part numbers to our internal part numbers

Create (dynamic) pricing pages for customers so sales people spend less time manually inputting or calculating costs or prices.

Create SQL Triggers (and audit tables) and get e-mail notifications about different issues (same-day orders, data issues when making purchase orders, alerting management about orders made with a high-priority customer, etc.)
