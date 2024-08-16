
# MarU Project Notebooks

This repository contains all the Databricks notebooks used in the MarU project. The notebooks are organized into various categories, each responsible for specific functions within the project.

## Table of Contents

- [Overview](#overview)
- [Notebooks](#notebooks)
  - [Main Notebooks](#main-notebooks)
  - [Consumption Calculations](#consumption-calculations)
  - [Emission Calculations](#emission-calculations)
  - [Emission Variables](#emission-variables)
  - [Vessel Data](#vessel-data)
  - [Utility Functions](#utility-functions)
- [Data Flow](#data-flow)

## Overview

The MarU project is designed to calculate emissions from vessels based on various factors like fuel consumption, engine load, and geographic location. The notebooks in this repository facilitate these calculations, providing data that can be used in tools for reporting and analysis.

## Notebooks

### Main Notebooks

- **maru**: The main notebook that runs the model.
- **maru_grouped**: A report with grouped data used by the frontend application.

### Area
- **area / areas**: Geographical areas.

### Consumption Calculations

- **consumption / fuel**: Calculates fuel consumption based on specific fuel consumption (SFC) and kWh.
- **consumption / kwh**: Calculates the engine's load factor (`main_engine_load_factor`) based on speed and theoretical maximum speed. It also calculates energy consumption for the main engine and auxiliary engines/boiler.
- **src /consumption / sfc**: Sets specific fuel consumption (SFC) based on emission factors.

### Emission Calculations

- **emission / bc**: Calculates Black Carbon (BC) emissions, differentiated by whether the AIS position is inside or outside an ECA area.
- **emission / ch4**: Calculates methane (CH4) emissions.
- **emission / co**: Calculates carbon monoxide (CO) emissions.
- **emission / co2**: Sets values for Carbon conversion factors and calculates CO2 emissions.
- **emission / fsf**: Sets values for Fuel Sulfur Fraction.
- **emission / n2o**: Calculates nitrous oxide (N2O) emissions.
- **emission / nmvoc**: Calculates Non-Methane Volatile Organic Compounds (NMVOC) emissions.
- **emission / nox**: Calculates nitrogen oxides (NOx) emissions, differentiated by ECA area.
- **emission / pm**: Calculates Particulate Matter (PM) emissions, including PM10 and PM2.5.
- **emission / sox**: Calculates sulfur oxide (SOx) emissions.
- **emission / low_load**: Applies a low load factor when the `me_load_factor` is below 0.2, multiplying it with the `low_load_factor`.
- **emission / total_emission**: Sums up emission estimates for the main engine, auxiliary engines, and boiler.

### Emission Variables

- **emission_variables / emission_variables_bronze**: Loads data from raw to bronze.
- **emission_variables / emission_variables_silver**: Loads data from bronze to silver.
- **emission_variables / transformations**: Transformations for emission variables.

### Utility Functions

- **utils / read_data**: Common functions used across multiple notebooks.

### Vessel Data

- **vessel / vessel_aux_boiler / vessel_aux_boiler_bronze**: Loads auxiliary boiler data from raw to bronze.
- **vessel / vessel_aux_boiler / vessel_aux_boiler_silver**: Loads auxiliary boiler data from bronze to silver.
- **vessel / vessel_battery / transformations**: Transformations for vessel battery data.
- **vessel / vessel_battery / vessel_battery_bronze**: Loads battery data from raw to bronze.
- **vessel / vessel_battery / vessel_battery_silver**: Loads battery data from bronze to silver.
- **vessel / vessel_gold / vessel**: The MarU ship register, an enrichment of Shipdata Combined.
- **vessel / vessel_type / vessel_type_bronze**: Loads vessel type data from raw to bronze.
- **vessel / vessel_type / vessel_type_silver**: Loads vessel type data from bronze to silver.

## Data Flow

![Data flow](data_flow_detailed.png)
