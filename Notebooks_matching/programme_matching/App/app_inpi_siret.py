import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import pandas as pd
import numpy as np
import sqlite3
import base64


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
image_filename = "calf1.png" # replace with your own image
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
    html.H1('INPI'),
    html.P([ html.Br()]),
    dcc.Input(id='insert_siren',
    value='insert SIREN',
    type='text'),
    html.Hr(),
    dcc.Dropdown(
            id='choose_city',
            ),
    html.Div(id='my-div'),
    html.P([ html.Br()]),
    html.Iframe(id = 'datatable', height = 500, width = 1200)
])


@app.callback(
    Output('choose_city', 'options'),
    [Input('insert_siren', 'value')])
def set_cities_options(selected_siren):
    conn = conn = sqlite3.connect(r"C:\Users\PERNETTH\Documents\Projects" \
    r"\InseeInpi_matching\Notebooks_matching" \
    r"\programme_matching\App\SQL\siren_inpi.db")
    c = conn.cursor()
    query = '''SELECT * FROM SIREN WHERE siren = {} '''.format(selected_siren)

    c.execute(query)

    df = pd.DataFrame(c.fetchall(), columns= cols).reindex(columns =
        reindex_cols)['ncc'].drop_duplicates().loc[lambda x:
        ~x.isin([None])].to_list()

    return [{'label':name, 'value':name} for name in df]

@app.callback(
    Output(component_id='datatable', component_property='srcDoc'),
    [Input(component_id='insert_siren', component_property='value'),
    Input(component_id='choose_city', component_property='value'),
    ]
)
def update_output_div(siren, city):
    conn = conn = sqlite3.connect(r"C:\Users\PERNETTH\Documents\Projects" \
    r"\InseeInpi_matching\Notebooks_matching" \
    r"\programme_matching\App\SQL\siren_inpi.db")
    c = conn.cursor()
    query = '''SELECT * FROM SIREN WHERE siren = {} AND ncc = '{}' '''.format(
    siren, city)
    c.execute(query)
    df = pd.DataFrame(c.fetchall(), columns= cols).reindex(columns =
    reindex_cols)
    return df.to_html()


if __name__ == '__main__':
    app.run_server(debug=True)
