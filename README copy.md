# ContiStreamlitApp

## To run locally
pip install -r requirements.txt
streamlit run <Your_Path_To_C_Drive>contistreamlitapp\contistreamlitapp\Conti_Streamlit_App.py


## Docker Commands to run the app
docker build -t streamlit-conti-app .              

docker run -p 8501:8501 streamlit-conti-app

## Command to deploy to Azure Container Registry

az acr build --registry energetechnew --image streamlit-conti-app .

## Issues 

* Duplicates entry on BSADs live fixed


Changelog:

Test