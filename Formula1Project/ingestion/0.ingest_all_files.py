# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

v_circuit = dbutils.notebook.run("1.ingest_circuits_file", 0, {
    "p_data_source": "Ergast API"
})
v_race = dbutils.notebook.run("2.race_assignment", 0, {
    "p_data_source": "Ergast API"
})
v_constructors = dbutils.notebook.run("3.ingest_constructors_file", 0, {
    "p_data_source": "Ergast API"
})
v_drivers = dbutils.notebook.run("4.ingest_drivers_file", 0, {
    "p_data_source": "Ergast API"
})
v_results = dbutils.notebook.run("5.results_assignment", 0, {
    "p_data_source": "Ergast API"
})
v_pit_stops = dbutils.notebook.run("6.ingest_pit_stops_multiline_file", 0, {
    "p_data_source": "Ergast API"
})
v_lap_times = dbutils.notebook.run("7.ingest_lap_times_file", 0, {
    "p_data_source": "Ergast API"
})
v_qualifying = dbutils.notebook.run("8.ingest_qualifying_file", 0, {
    "p_data_source": "Ergast API"
})

# COMMAND ----------

v_circuit
v_race
v_constructors
v_drivers
v_results
v_pit_stops
v_lap_times
v_qualifying
