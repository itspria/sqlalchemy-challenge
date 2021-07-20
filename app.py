# import Flask
from flask import Flask, jsonify

import numpy as np
import pandas as pd
import datetime as dt

# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

# create engine to hawaii.sqlite
engine = create_engine("sqlite:///hawaii.sqlite")
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create an app, being sure to pass __name__
app = Flask(__name__)

# Define what to do when a user hits the index route
@app.route("/")
def home():
    print("Server received request for root")
    return (
        f"<h1>Welcome to the Climate api!</h1><br>"
        f"<b>To view the precipitation use the following api</b><br>"
        f"/api/v1.0/precipitation<br><br>"
        f"<b>To view the stations use the following api</b><br>"
        f"/api/v1.0/stations<br><br>"
        f"<b>To view the tobs use the following api</b><br>"
        f"/api/v1.0/tobs<br><br>"
        f"<b>To view the minimum, average and maximum temperature, for a given start or start-end date range use the following api</b><br>"
        f"<em>Enter dates in the range 2010-01-01 and 2017-08-23  </em><br>"
        f"/api/v1.0/&ltstart&gt<br>"
        f"/api/v1.0/&ltstart&gt/&ltend&gt<br>")

@app.route("/api/v1.0/precipitation")
def precipitation():
    print("Server received request for processing precipitation data...")
    session = Session(engine)

    #Calculate the date range
    recentDate = session.query(func.max(Measurement.date)).first()
    dateRange = dt.datetime.strptime(recentDate[0], '%Y-%m-%d') - dt.timedelta(days=365)

    #Get the precipitation data
    rows = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= dateRange).all()
    session.close()
    results =[]

    for row in rows:
        result = {}
        result["Date"] = row[0]
        result["Precipitation"] = row[1]
        results.append(result)
    

    return jsonify(results)

@app.route("/api/v1.0/stations")
def stations():
    print("Server received request for processing stations data...")
    session = Session(engine)
    rows = session.query(Station.name).all()
    session.close()
    results = list(np.ravel(rows))
    
    return jsonify(results)

@app.route("/api/v1.0/tobs")
def tobs():
    print("Server received request for processing tobs...")
    session = Session(engine)
    
    #Get active station id
    results = session.query(Measurement.station,Station.name, func.count(Measurement.station))\
        .filter(Measurement.station == Station.station)\
        .group_by(Measurement.station)\
        .order_by(func.count(Measurement.station).desc()).all()
    activeStation = results[0][0]

    #Calulate the date range
    recentDate = session.query(func.max(Measurement.date)).filter(Measurement.station == activeStation).first()
    dateRange = dt.datetime.strptime(recentDate[0], '%Y-%m-%d') - dt.timedelta(days=365)

    #Get TOBS data
    rows = session.query(Measurement.date, Measurement.tobs).filter(Measurement.station == activeStation).filter(Measurement.date >= dateRange).all()
    session.close()

    data =[]

    for row in rows:
        result = {}
        result["Date"] = row[0]
        result["TOBS"] = row[1]
        data.append(result)
    
    return jsonify(data)

@app.route("/api/v1.0/<startDate>")
def tobsWithStartDate(startDate):
    #print("Server received request for processing tobs for the start date ", startDate,"...")
    session = Session(engine)

    results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).filter(Measurement.date >= startDate).all()
    session.close()

    result = {}
    result['TMIN'] = results[0][0]
    result['TAVG'] = results[0][1]
    result['TMAX'] = results[0][2]
    return jsonify(result)

    return jsonify({"error": f"TOBS data for date {startDate} not found."}), 404

@app.route("/api/v1.0/<startDate>/<endDate>")
def tobsWithstartDateEndDate(startDate,endDate):
    session = Session(engine)

    results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).filter(Measurement.date >= startDate).filter(Measurement.date <= endDate).all()
    session.close()

    result = {}
    result['TMIN'] = results[0][0]
    result['TAVG'] = results[0][1]
    result['TMAX'] = results[0][2]
    return jsonify(result)

    return jsonify({"error": f"TOBS data for dates {startDate} - {endDate} not found."}), 404

# Define main behavior
if __name__ == "__main__":
    app.run(debug=True)
