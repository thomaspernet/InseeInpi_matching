import dash, sqlite3, base64, webbrowser
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import pandas as pd
import numpy as np
from threading import Timer


external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
image_filename = r"C:\Users\PERNETTH\Documents\Projects\InseeInpi_matching" \
r"\Notebooks_matching\programme_matching\App\calf1.png"

encoded_image = base64.b64encode(open(image_filename, 'rb').read())

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
cols = ["Code" "Greffe","Nom_Greffe","Numero_Gestion","siren","Type",
"Siège_PM","RCS_Registre","Adresse_Ligne1","Adresse_Ligne2",
"Adresse_Ligne3","Code_Postal","Ville","Code_Commune","Pays",
"Domiciliataire_Nom","Domiciliataire_Siren","Domiciliataire_Greffe",
"Domiciliataire_Complément","Siege_Domicile_Représentant","Nom_Commercial",
"Enseigne","Activité_Ambulante","Activité_Saisonnière","Activité_Non_Sédentaire",
"Date_Début_Activité","Activité","Origine_Fonds","Origine_Fonds_Info",
"Type_Exploitation","ID_Etablissement","Date_Greffe","Libelle_Evt",
"count_initial_inpi","ncc","Adress_new","Adresse_new_clean_reg", "digit_inpi",
"possibilite","INSEE","siret","count_initial_insee","origin","count_siren_siret",
"test_address_libelle","test_address_complement","test_join_address",
"test_date","test_1","test_siege","test_voie","test_numero",
"count_duplicates_final","count_duplicates_","test"]

reindex_cols =["Code" "Greffe","Nom_Greffe","Numero_Gestion","siren","siret",
"Type",
"Siège_PM","RCS_Registre","Adresse_Ligne1","Adresse_Ligne2",
"Adresse_Ligne3","Code_Postal","Ville","Code_Commune","Pays",
"Domiciliataire_Nom","Domiciliataire_Siren","Domiciliataire_Greffe",
"Domiciliataire_Complément","Siege_Domicile_Représentant","Nom_Commercial",
"Enseigne","Activité_Ambulante","Activité_Saisonnière","Activité_Non_Sédentaire",
"Date_Début_Activité","Activité","Origine_Fonds","Origine_Fonds_Info",
"Type_Exploitation","ID_Etablissement","Date_Greffe","Libelle_Evt",
"count_initial_inpi","ncc","Adress_new","Adresse_new_clean_reg", "digit_inpi",
"possibilite","INSEE","count_initial_insee","origin","count_siren_siret",
"test_address_libelle","test_address_complement","test_join_address",
"test_date","test_1","test_siege","test_voie","test_numero",
"count_duplicates_final","count_duplicates_","test"]


#all_options = {
    #'America': ['New York City', 'San Francisco', 'Cincinnati'],
    #'Canada': [u'Montréal', 'Toronto', 'Ottawa']
#}
app.layout = html.Div([
    html.H1('INPI-SIRET'),
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
    r"\programme_matching\App\SQL\inpi_matched.db")
    c = conn.cursor()
    query = '''SELECT * FROM INPI_MATCHED WHERE siren = {} '''.format(selected_siren)

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
    r"\programme_matching\App\SQL\inpi_matched.db")
    c = conn.cursor()
    query = '''SELECT * FROM INPI_MATCHED WHERE siren = {} AND ncc = '{}' '''.format(
    siren, city)
    c.execute(query)
    df = pd.DataFrame(c.fetchall(), columns= cols).reindex(columns =
    reindex_cols)
    return df.to_html()

def open_browser():
	webbrowser.open_new("http://localhost:{}".format(port))

def is_port_in_use(port):
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

port = 8050
port_test = True
while port_test:
    port+=1
    port_test = is_port_in_use(port= port)

if __name__ == '__main__':
    Timer(1, open_browser).start()
    app.run_server(debug=False, port=port)
