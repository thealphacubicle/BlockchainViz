# Introduction
Welcome to the BlockChain Visualization project! This project aims to visualize the 
blockchain data in a more intuitive way. The project is divided into two parts: the 
backend and the frontend. The backend is responsible for fetching the blockchain data 
and processing it, while the frontend is responsible for visualizing the processed data.
The front-end consists of Tableau dashboards that visualize the blockchain data. 
The back-end is a Python script that fetches the blockchain data and is stored inside
a MongoDB Atlas instance. 

I have also written a Medium article that explains the project in more detail. See [here](https://medium.com/@srihari.raman/a-real-time-discovery-of-bitcoin-through-tableau-345c94681cc1)
for more details!

## Collecting the Data
The data is collected using the `pipeline.py` script. The script fetches the data from
the [blockchair.com](https://www.blockchair.com) API. The data is then stored in a MongoDB
Atlas instance. Currently, the script does not run automatically, but the feature will be
added in the future.

## Visualizing the Data
The data is visualized using Tableau dashboards. The current version of the dashboards
looks like this:
![Tableau Dashboard](diagrams/mvp_screenshot.png)

## Collaborators
Currently, the project is not open for collaboration. However, the project will be open
in the near future to allow for more contributors and added functionality.

---
# Installation
#### Step 1: Clone the repository using SSH
```bash
git clone git@github.com:thealphacubicle/BlockchainViz.git 
```

#### Step 2: Create a virtual environment (preferably with Conda)
```bash
conda create -n blockchainviz python=3.8
```

#### Step 3: Activate the virtual environment
```bash
conda activate blockchainviz
```

#### Step 4: Install the dependencies
```bash
pip install -r requirements.txt
```

#### Step 5: Create a `keys.env` file in the root directory and add the following keys:
```bash
MONGO_USERNAME=<your_mongo_username>
MONGO_PWD = <your_mongo_password>
```

#### Step 6: Run the `pipeline.py` script
```bash
python pipeline.py
```

#### Step 7: Open the Tableau dashboards and Connect to your MongoDB Atlas DB Instance
- You will need to install the MongoDB ODBC driver to connect to the MongoDB Atlas instance (see [here](https://www.youtube.com/watch?v=vEgIB44kKL0)).
- Once the driver is installed, open Tableau and connect to the MongoDB Atlas instance using the ODBC driver.
- You can now visualize the data using the Tableau dashboards!
