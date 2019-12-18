#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Service functions for OEP logging

Read data from MaStR API, process, and write to OEP

SPDX-License-Identifier: AGPL-3.0-or-later
"""

__copyright__ = "© Reiner Lemoine Institut"
__license__ = "GNU Affero General Public License Version 3 (AGPL-3.0)"
__url__ = "https://www.gnu.org/licenses/agpl-3.0.en.html"
__author__ = "Ludee; christian-rli"
__issue__ = "https://github.com/OpenEnergyPlatform/examples/issues/52"
__version__ = "v0.9.0"

import config as lc

# import getpass
import os
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean
from collections import namedtuple
import requests
import urllib3
import oedialect
import pandas as pd
import numpy as np
from soap_api.utils import fname_hydro, fname_wind, fname_hydro, fname_biomass


from zeep import Client, Settings
from zeep.cache import SqliteCache
from zeep.transports import Transport

UserToken = namedtuple('UserToken', ['user', 'token'])
API_MAX_DEMANDS = 2000

import logging
log = logging.getLogger(__name__)


def oep_upload():

    files = [(fname_hydro, 'hydro'),(fname_wind, 'wind'), (fname_biomass, 'biomass')]
    log.info('starting OEP upload')
    # 2nd, check which powerunit files are 'maked'
    for file in files:
        log.info(file[0])
        if os.path.exists(file[0]):
            # 3rd, insert data into tables
            """dtypes = [float, float, float, np.object, np.object, np.object, np.object, np.object, float, float, np.object, 
            np.object, np.object, np.object, np.object, np.object, np.object, float, float, np.object, np.object, float, np.object, np.object, float, float,
            np.object, np.object, np.object, np.object, np.object, np.object, np.object, np.object, np.object, float, np.object, np.object, np.object, np.object, np.object, np.object, np.object,
            np.object, np.object,  float, float, float, float, float, float, float, np.object, float, np.object, np.object, np.object,
            np.object, np.object, float, float, np.object, np.object, np.object, float, np.object, np.object, np.object, np.object, np.object, float, float,
            np.object, float, float, float, float, float, float, float, float, np.object, np.object, np.object, np.object, np.object,
            np.object, np.object, np.object, float, float, float, np.object, float, float, float, float, float, float,
            float ,float, np.object, float, np.object, np.object, np.object, float, np.object, np.object, float, float, np.object, np.object,
            np.object, np.object, np.object, np.object, np.object, float, np.object, np.object, np.object, np.object, np.object, np.object, np.object, np.object, np.object, np.object, np.object,
            np.object, np.object, np.object, np.object, np.object, np.object, np.object, np.object, np.object, np.object, np.object, np.object]"""
            upload_file = pd.read_csv(file[0], encoding='utf8', sep=';', low_memory=False, dtype={"w-id": float, 'pu-id': float, 'lid': float})
        
            #with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            #    print(upload_file.dtypes)
            insert_data(file[0], upload_file)
        else:
            log.info('file not found')

def oep_config():
    """Access config.ini.

    Returns
    -------
    UserToken : namedtuple
        API token (key) and user name (value).
    """
    config_section = 'OEP'

    # username
    try:
        lc.config_file_load()
        user = lc.config_file_get(config_section, 'user')
        log.info(f'Hello {user}, welcome back')
    except FileNotFoundError:
        user = input('Please provide your OEP username (default surname_name):')
        log.info(f'Hello {user}')

    # token
    try:
        token = lc.config_file_get(config_section, 'token')
        print(f'Load API token')
    except:
        import sys
        #token = input('Token:')
        token = getpass.getpass('Token:')
        #prompt = 'Token:',
        #                         stream = sys.stdin)
        lc.config_section_set(config_section, value=user, key=token)
        log.info('Config file created')
    return UserToken(user, token)


# a = oep_config()S
# a.user
# a.token

def oep_session():
    """SQLAlchemy session object with valid connection to database.

    Returns
    -------
    metadata : SQLAlchemy object
        Database connection object.
    """
    user, token = oep_config()
    # engine
    #try:
    oep_url = 'openenergy-platform.org'  # 'oep.iks.cs.ovgu.de'
    oed_string = f'postgresql+oedialect://{user}:{token}@{oep_url}'
    engine = sa.create_engine(oed_string)
    metadata = sa.MetaData(bind=engine)
    print(f'OEP connection established: /n {metadata}')
    return [metadata, engine]


def get_table_powerunits(metadata):
    metadata = metadata.reflect()
    pu_table = metadata.tables['table name powerunits']
    return pu_table


def insert_data(table_name, upload_file):
    ''' configure oep session '''
    [metadata, engine] = oep_session()
    log.info('Connection established')
    schema_name = 'sandbox'
    log.info(f'table name: {table_name}')
    """
    measurer = np.vectorize(len)
    max_len = dict(zip(upload_file, measurer(upload_file.values.astype(str)).max(axis=0)))
    print(max_len)
    """
    ''' Create Table Schema ''' 
    mastr_table = sa.Table(
        table_name, 
        metadata,    
        sa.Column("w-id" ,sa.Float(10)),
        sa.Column("pu-id", sa.Float(10)),
        sa.Column("lid", sa.Float(10)),
        sa.Column("EinheitMastrNummer",sa.String(20)),
        sa.Column("Name", sa.String(100)),
        sa.Column("Einheitart" ,sa.String(25)),
        sa.Column("Einheittyp" ,sa.String(15)),
        sa.Column("Standort" ,sa.String(150)),
        sa.Column("Bruttoleistung" ,sa.Float(10)),
        sa.Column("Erzeugungsleistung" ,sa.Float(10)),
        sa.Column("EinheitBetriebsstatus",sa.String(25)),
        sa.Column("Anlagenbetreiber",sa.String(20)),
        sa.Column("EegMastrNummer",sa.String(20)),
        sa.Column("KwkMastrNummer",sa.Float(10)),
        sa.Column("SpeMastrNummer" ,sa.Float(10)),
        sa.Column("GenMastrNummer" ,sa.String(20)),
        sa.Column("BestandsanlageMastrNummer",sa.Float(10)),
        sa.Column("NichtVorhandenInMigriertenEinheiten" ,sa.Float(5)),
        sa.Column("StatistikFlag", sa.Float(5)),
        sa.Column("version" ,sa.String(10)),
        sa.Column("timestamp" ,sa.String(30)),

        sa.Column("lid_w" ,sa.Float(5)),

        sa.Column("Ergebniscode" ,sa.String(5)),
        sa.Column("AufrufVeraltet" ,sa.String(5)),

        sa.Column("AufrufLebenszeitEnde" ,sa.Float(5)),
        sa.Column("AufrufVersion" ,sa.Float(5)),

        sa.Column("DatumLetzteAktualisierung" ,sa.String(30)),
        sa.Column("LokationMastrNummer" ,sa.String(20)),
        sa.Column("NetzbetreiberpruefungStatus" ,sa.String(15)),
        sa.Column("NetzbetreiberpruefungDatum",sa.String(15)),
        sa.Column("NetzbetreiberMastrNummer" ,sa.String(20)),
        sa.Column("Land" ,sa.String(15)),
        sa.Column("Bundesland",sa.String(35)),
        sa.Column("Landkreis",sa.String(35)),
        sa.Column("Gemeinde",sa.String(35)),

        sa.Column("Gemeindeschluessel" ,sa.Float(10)),

        sa.Column("Postleitzahl",sa.String(5)),
        sa.Column("Gemarkung" ,sa.String(70)),
        sa.Column("FlurFlurstuecknummern" ,sa.String(260)),
        sa.Column("Strasse" ,sa.String(100)),
        sa.Column("StrasseNichtGefunden",sa.String(10)),
        sa.Column("Hausnummer",sa.String(65)),
        sa.Column("HausnummerNichtGefunden",sa.String(5)),
        sa.Column("Adresszusatz" ,sa.String(50)),
        sa.Column("Ort",sa.String(55)),

        sa.Column("Laengengrad" ,sa.Float(10)),
        sa.Column("Breitengrad",sa.Float(15)),
        sa.Column("UtmZonenwert" ,sa.Float(10)),
        sa.Column("UtmEast" ,sa.Float(20)),
        sa.Column("UtmNorth" ,sa.Float(20)),
        sa.Column("GaussKruegerHoch" ,sa.Float(20)),
        sa.Column("GaussKruegerRechts",sa.Float(20)),

        sa.Column("Meldedatum" ,sa.String(10)),

        sa.Column("GeplantesInbetriebnahmedatum",sa.Float(5)),

        sa.Column("Inbetriebnahmedatum",sa.String(10)),
        sa.Column("DatumEndgueltigeStilllegung" ,sa.String(10)),
        sa.Column("DatumBeginnVoruebergehendeStilllegung",sa.String(10)),
        sa.Column("DatumWiederaufnahmeBetrieb" ,sa.String(10)),
        sa.Column("EinheitBetriebsstatus_w",sa.String(25)),

        sa.Column("BestandsanlageMastrNummer_w" ,sa.Float(5)),
        sa.Column("NichtVorhandenInMigriertenEinheiten_w" ,sa.Float(5)),

        sa.Column("AltAnlagenbetreiberMastrNummer", sa.String(15)),
        sa.Column("DatumDesBetreiberwechsels", sa.String(10)),
        sa.Column("DatumRegistrierungDesBetreiberwechsels", sa.String(10)),
        sa.Column("StatisikFlag_w", sa.String(5)),
        sa.Column("NameStromerzeugungseinheit",sa.String(90)),
        sa.Column("Weic" ,sa.String(75)),
        sa.Column("WeicDisplayName",sa.String(40)),
        sa.Column("Kraftwerksnummer",sa.String(60)),
        sa.Column("Energietraeger" ,sa.String(5)),

        sa.Column("Bruttoleistung_w",sa.Float(20)),
        sa.Column("Nettonennleistung" ,sa.Float(20)),

        sa.Column("AnschlussAnHoechstOderHochSpannung" ,sa.String(10)),

        sa.Column("Schwarzstartfaehigkeit" ,sa.Float(5)),
        sa.Column("Inselbetriebsfaehigkeit" ,sa.Float(5)),
        sa.Column("Einsatzverantwortlicher",sa.Float(5)),

        sa.Column("FernsteuerbarkeitNb" ,sa.String(5)),
        sa.Column("FernsteuerbarkeitDv",sa.String(5)),
        sa.Column("FernsteuerbarkeitDr" ,sa.String(5)),
        sa.Column("Einspeisungsart" ,sa.String(15)),

        sa.Column("PraequalifiziertFuerRegelenergie" ,sa.Float(5)),

        sa.Column("GenMastrNummer_w" ,sa.String(15)),
        sa.Column("NameWindpark",sa.String(95)),
        sa.Column("Lage" ,sa.String(15)),
        sa.Column("Seelage" ,sa.String(10)),
        sa.Column("ClusterOstsee" ,sa.String(5)),
        sa.Column("ClusterNordsee" ,sa.String(5)),
        sa.Column("Technologie" ,sa.String(20)),
        sa.Column("Typenbezeichnung" ,sa.String(60)),

        sa.Column("Nabenhoehe" ,sa.Float(10)),
        sa.Column("Rotordurchmesser" ,sa.Float(10)),
        sa.Column("Rotorblattenteisungssystem" ,sa.Float(10)),

        sa.Column("AuflageAbschaltungLeistungsbegrenzung" ,sa.String(5)),

        sa.Column("AuflagenAbschaltungSchallimmissionsschutzNachts" ,sa.Float(5)),
        sa.Column("AuflagenAbschaltungSchallimmissionsschutzTagsueber" ,sa.Float(5)),
        sa.Column("AuflagenAbschaltungSchattenwurf" ,sa.Float(5)),
        sa.Column("AuflagenAbschaltungTierschutz" ,sa.Float(5)),
        sa.Column("AuflagenAbschaltungEiswurf" ,sa.Float(5)),
        sa.Column("AuflagenAbschaltungSonstige" ,sa.Float(5)),
        sa.Column("Wassertiefe",sa.Float(5)),
        sa.Column("Kuestenentfernung",sa.Float(20)),

        sa.Column("EegMastrNummer_w" ,sa.String(15)),
        sa.Column("HerstellerID" ,sa.Float(5)),

        sa.Column("HerstellerName" ,sa.String(80)),
        sa.Column("version_w" ,sa.String(10)),
        sa.Column("timestamp_w" ,sa.String(30)),

        sa.Column("lid_e" ,sa.Float(5)),

        sa.Column("Ergebniscode_e" ,sa.String(5)),
        sa.Column("AufrufVeraltet_e" ,sa.String(5)),

        sa.Column("AufrufLebenszeitEnde_e" ,sa.Float(5)),
        sa.Column("AufrufVersion_e" ,sa.Float(5)),

        sa.Column("Meldedatum_e" ,sa.String(10)),
        sa.Column("DatumLetzteAktualisierung_e" ,sa.String(30)),
        sa.Column("EegInbetriebnahmedatum" ,sa.String(10)),
        sa.Column("AnlagenkennzifferAnlagenregister" ,sa.String(75)),
        sa.Column("AnlagenschluesselEeg",sa.String(35)),
        sa.Column("PrototypAnlage" ,sa.String(5)),
        sa.Column("PilotAnlage" ,sa.String(5)),

        sa.Column("InstallierteLeistung" ,sa.Float(20)),

        sa.Column("VerhaeltnisErtragsschaetzungReferenzertrag",sa.String(80)),
        sa.Column("VerhaeltnisReferenzertragErtrag5Jahre",sa.String(80)),
        sa.Column("VerhaeltnisReferenzertragErtrag10Jahre" ,sa.String(80)),
        sa.Column("VerhaeltnisReferenzertragErtrag15Jahre" ,sa.String(80)),
        sa.Column("AusschreibungZuschlag" ,sa.String(5)),
        sa.Column("Zuschlagsnummer" ,sa.String(15)),
        sa.Column("AnlageBetriebsstatus",sa.String(25)),
        sa.Column("VerknuepfteEinheit" ,sa.String(130)),
        sa.Column("version_e",sa.String(10)),
        sa.Column("timestamp_e" ,sa.String(30)),
        sa.Column("MaStRNummer" ,sa.String(20)),
        sa.Column("Einheittyp_p" ,sa.String(15)),
        sa.Column("Einheitart_p" ,sa.String(30)),
        sa.Column("Datum" ,sa.String(10)),
        sa.Column("Art" ,sa.String(30)),
        sa.Column("Behoerde" ,sa.String(180)),
        sa.Column("Aktenzeichen" ,sa.String(100)),
        sa.Column("Frist" ,sa.String(15)),

        sa.Column("WasserrechtsNummer" ,sa.Float(5)),
        sa.Column("WasserrechtAblaufdatum" ,sa.Float(5)),
        sa.Column("Meldedatum_p" ,sa.String(10)),
        sa.Column("version_m" ,sa.String(10)),
        sa.Column("timestamp_m" ,sa.String(30)),

        schema=schema_name)

    log.info('table schema created')
    con = engine.connect()
    log.info('engine connect')
    if not engine.dialect.has_table(connection=con,table_name=table_name, schema=schema_name):
        log.info('trying to create table')
        #metadata.create_all(engine)
        mastr_table.create()
        print('Created table')
    else:
        print('Table already exists')

    try: 
        upload_file.to_sql(table_name, con, schema_name, if_exists='replace', chunksize=100)
        print('Inserted to ' + table_name)
    except Exception as e:
        session.rollback()
        raise
        print('Insert incomplete!')




def mastr_config():
    """Access config.ini.

    Returns
    -------
    user : str
        marktakteurMastrNummer (value).
    token : str
        API token (key).
    """
    config_section = 'MaStR'

    # user
    try:
        lc.config_file_load()
        user = lc.config_file_get(config_section, 'user')
        # print('Hello ' + user)
    except:
        user = input('Please provide your MaStR Nummer:')

    # token
    try:
        from config import config_file_get
        token = config_file_get(config_section, 'token')
    except:
        import sys
        token = input('Token:')
        # token = getpass.getpass(prompt='apiKey: ',
        #                            stream=sys.stderr)
        lc.config_section_set(config_section, value=user, key=token)
        print('Config file created.')
    return user, token


def mastr_session():
    """MaStR SOAP session using Zeep Client.

    Returns
    -------
    client : SOAP client
        API connection.
    client_bind : SOAP client bind
        bind API connection.
    token : str
        API key.
    user : str
        marktakteurMastrNummer.
    """
    user, token = mastr_config()

    wsdl = 'https://www.marktstammdatenregister.de/MaStRAPI/wsdl/mastr.wsdl'
    session = requests.Session()
    session.max_redirects = 30
    a = requests.adapters.HTTPAdapter(max_retries=3, pool_connections=2000, pool_maxsize=2000)
    session.mount('https://',a)
    session.mount('http://',a)
    transport = Transport(cache=SqliteCache(), timeout=600, session=session)
    settings = Settings(strict=True, xml_huge_tree=True)
    client = Client(wsdl=wsdl, transport=transport, settings=settings)
    client_bind = client.bind('Marktstammdatenregister', 'Anlage')

    mastr_suppress_parsing_errors(['parse-time-second'])

    # print(f'MaStR API connection established for user {user}')
    return client, client_bind, token, user


def mastr_suppress_parsing_errors(which_errors):
    """
    Install logging filters into zeep type parsing modules to suppress

    Arguments
    ---------
    which_errors : [str]
        Names of errors defined in `error_filters` to set up.
        Currently one of ('parse-time-second').

    NOTE
    ----
    zeep and mastr don't seem to agree on the correct time format. Instead of
    suppressing the error, we should fix the parsing error, or they should :).
    """

    class FilterExceptions(logging.Filter):
        def __init__(self, name, klass, msg):
            super().__init__(name)

            self.klass = klass
            self.msg = msg

        def filter(self, record):
            if record.exc_info is None:
                return 1

            kl, inst, tb = record.exc_info
            return 0 if isinstance(inst, self.klass) and inst.args[0] == self.msg else 1

    # Definition of available filters
    error_filters = [FilterExceptions('parse-time-second', ValueError, 'second must be in 0..59')]

    # Install filters selected by `which_errors`
    zplogger = logging.getLogger('zeep.xsd.types.simple')
    zplogger.filters = ([f for f in zplogger.filters if not isinstance(f, FilterExceptions)] +
                        [f for f in error_filters if f.name in which_errors])