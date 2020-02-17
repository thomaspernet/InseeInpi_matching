import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import pandas as pd
import numpy as np
import sqlite3
import base64


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
image_filename = r'C:\Users\PERNETTH\Documents\Projects\InseeInpi_matching\Notebooks_matching\programme_matching\App\calf.png' # replace with your own image
encoded_image = base64.b64encode(open(image_filename, 'rb').read())

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
cols = ["Code" "Greffe","Nom_Greffe","Numero_Gestion","siren","Type", \
 "Siège_PM","RCS_Registre","Adresse_Ligne1","Adresse_Ligne2","Adresse_Ligne3",  \
 "Code_Postal", "Ville","Code_Commune","Pays","Domiciliataire_Nom","Domiciliataire_Siren",  \
 "Domiciliataire_Greffe","Domiciliataire_Complément","Siege_Domicile_Représentant","Nom_Commercial",  \
 "Enseigne","Activité_Ambulante", "Activité_Saisonnière","Activité_Non_Sédentaire",  \
 "Date_Début_Activité","Activité", "Origine_Fonds","Origine_Fonds_Info",  \
 "Type_Exploitation","ID_Etablissement","Date_Greffe","Libelle_Evt", "count", "siret", "ncc", "Adress_new"]

reindex_cols = ["siren","Type","siret","ncc", "Adress_new", "Code" "Greffe","Nom_Greffe","Numero_Gestion", \
  "Siège_PM","RCS_Registre","Adresse_Ligne1","Adresse_Ligne2","Adresse_Ligne3",  \
  "Code_Postal", "Ville","Code_Commune","Pays","Domiciliataire_Nom","Domiciliataire_Siren",  \
  "Domiciliataire_Greffe","Domiciliataire_Complément","Siege_Domicile_Représentant","Nom_Commercial",  \
  "Enseigne","Activité_Ambulante", "Activité_Saisonnière","Activité_Non_Sédentaire",  \
  "Date_Début_Activité","Activité", "Origine_Fonds","Origine_Fonds_Info",  \
  "Type_Exploitation","ID_Etablissement","Date_Greffe","Libelle_Evt", "count"]
#all_options = {
    #'America': ['New York City', 'San Francisco', 'Cincinnati'],
    #'Canada': [u'Montréal', 'Toronto', 'Ottawa']
#}
app.layout = html.Div([
    html.Img(src='data:image/png;base64,{}'.format(encoded_image)),
    html.P([ html.Br()]),
    dcc.Input(id='my-id', value='initial value', type='text'),
    html.Div(id='my-div'),
    html.Iframe(id = 'datatable', height = 500, width = 1200)
])


@app.callback(
    Output(component_id='datatable', component_property='srcDoc'),
    [Input(component_id='my-id', component_property='value')]
)
def update_output_div(input_value):
    conn = sqlite3.connect(r'C:\Users\PERNETTH\Documents\Projects\InseeInpi_matching\Notebooks_matching\programme_matching\siren_inpi.db')
    c = conn.cursor()
    query = '''SELECT * FROM SIREN WHERE siren = "%s"''' % input_value
    c.execute(query)
    df = pd.DataFrame(c.fetchall(), columns= cols).reindex(columns =
    reindex_cols)
    return df.to_html()

if __name__ == '__main__':
    app.run_server(debug=True)
