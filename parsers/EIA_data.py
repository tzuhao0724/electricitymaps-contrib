# Reverse exchanges need to be multiplied by -1, since they are reported in the opposite direction
REVERSE_EXCHANGES = [
    "US-CA->MX-BC",
    "MX-BC->US-CAL-CISO",
    "CA-SK->US-CENT-SWPP",
    "CA-MB->US-MIDW-MISO",
    "CA-ON->US-MIDW-MISO",
    "CA-QC->US-NE-ISNE",
    "CA-NB->US-NE-ISNE",
    "CA-BC->US-NW-BPAT",
    "CA-AB->US-NW-NWMT",
    "CA-QC->US-NY-NYIS",
    "CA-ON->US-NY-NYIS",
    "MX-NE->US-TEX-ERCO",
    "MX-NO->US-TEX-ERCO",
    "US-SW-PNM->US-SW-SRP",  # For some reason EBA.SRP-PNM.ID.H exists in EIA, but PNM-SRP does not. Probably because it is unidirectional
]

NEGATIVE_PRODUCTION_THRESHOLDS_TYPE = {
    "default": -10,
    "coal": -50,
    "gas": -20,
    "solar": -100,
    "wind": -20,
    "unknown": -50,
}

# based on https://www.eia.gov/beta/electricity/gridmonitor/dashboard/electric_overview/US48/US48
# or https://www.eia.gov/opendata/qb.php?category=3390101
# List includes regions and Balancing Authorities.
REGIONS = {
    # Non-US regions - EIA
    "MX-BC": "CFE",
    "MX-NE": "CEN",
    "MX-NO": "CFE",
    "CA-SK": "SPC",
    "CA-MB": "MHEB",
    "CA-ON": "IESO",
    "CA-QC": "HQT",
    "CA-NB": "NBSO",
    "CA-BC": "BCHA",
    "CA-AB": "AESO",
    # New regions - EIA
    "US-CAL-BANC": "BANC",  # Balancing Authority Of Northern California
    "US-CAL-CISO": "CISO",  # California Independent System Operator
    "US-CAL-IID": "IID",  # Imperial Irrigation District
    "US-CAL-LDWP": "LDWP",  # Los Angeles Department Of Water And Power
    "US-CAL-TIDC": "TIDC",  # Turlock Irrigation District
    "US-CAR-CPLE": "CPLE",  # Duke Energy Progress East
    "US-CAR-CPLW": "CPLW",  # Duke Energy Progress West
    "US-CAR-DUK": "DUK",  # Duke Energy Carolinas
    "US-CAR-SC": "SC",  # South Carolina Public Service Authority
    "US-CAR-SCEG": "SCEG",  # South Carolina Electric & Gas Company
    "US-CAR-YAD": "YAD",  # Alcoa Power Generating, Inc. - Yadkin Division
    "US-CENT-SPA": "SPA",  # Southwestern Power Administration
    "US-CENT-SWPP": "SWPP",  # Southwest Power Pool
    "US-FLA-FMPP": "FMPP",  # Florida Municipal Power Pool
    "US-FLA-FPC": "FPC",  # Duke Energy Florida Inc
    "US-FLA-FPL": "FPL",  # Florida Power & Light Company
    "US-FLA-GVL": "GVL",  # Gainesville Regional Utilities
    "US-FLA-HST": "HST",  # City Of Homestead
    "US-FLA-JEA": "JEA",  # Jea
    #    "US-FLA-NSB": "NSB",  # Utilities Commission Of New Smyrna Beach, Decomissioned data is directly integrated in another balancing authority
    # Some solar plants within this zone are operated by Florida Power & Light, therefore on the map the zones got merged.
    "US-FLA-SEC": "SEC",  # Seminole Electric Cooperative
    "US-FLA-TAL": "TAL",  # City Of Tallahassee
    "US-FLA-TEC": "TEC",  # Tampa Electric Company
    "US-MIDA-PJM": "PJM",  # Pjm Interconnection, Llc
    "US-MIDW-AECI": "AECI",  # Associated Electric Cooperative, Inc.
    # "US-MIDW-GLHB": "GLHB",  # GridLiance US-MIDW-GLHB decommissioned no more powerplant
    "US-MIDW-LGEE": "LGEE",  # Louisville Gas And Electric Company And Kentucky Utilities
    "US-MIDW-MISO": "MISO",  # Midcontinent Independent Transmission System Operator, Inc..
    "US-NE-ISNE": "ISNE",  # Iso New England Inc.
    "US-NW-AVA": "AVA",  # Avista Corporation
    "US-NW-AVRN": "AVRN",  # Avangrid Renewables, LLC, integrated with US-NW-BPAT and US-NW-PACW
    "US-NW-BPAT": "BPAT",  # Bonneville Power Administration
    "US-NW-CHPD": "CHPD",  # Public Utility District No. 1 Of Chelan County
    "US-NW-DOPD": "DOPD",  # Pud No. 1 Of Douglas County
    "US-NW-GCPD": "GCPD",  # Public Utility District No. 2 Of Grant County, Washington
    "US-NW-GRID": "GRID",  # Gridforce Energy Management, Llc
    "US-NW-GWA": "GWA",  # Naturener Power Watch, Llc (Gwa), integrated with US-NW-NWMT
    "US-NW-IPCO": "IPCO",  # Idaho Power Company
    "US-NW-NEVP": "NEVP",  # Nevada Power Company
    "US-NW-NWMT": "NWMT",  # Northwestern Energy (Nwmt)
    "US-NW-PACE": "PACE",  # Pacificorp - East
    "US-NW-PACW": "PACW",  # Pacificorp - West
    "US-NW-PGE": "PGE",  # Portland General Electric Company
    "US-NW-PSCO": "PSCO",  # Public Service Company Of Colorado
    "US-NW-PSEI": "PSEI",  # Puget Sound Energy
    "US-NW-SCL": "SCL",  # Seattle City Light
    "US-NW-TPWR": "TPWR",  # City Of Tacoma, Department Of Public Utilities, Light Division
    "US-NW-WACM": "WACM",  # Western Area Power Administration - Rocky Mountain Region
    "US-NW-WAUW": "WAUW",  # Western Area Power Administration Ugp West
    "US-NW-WWA": "WWA",  # Naturener Wind Watch, Llc, integrated with US-NW-NWMT
    "US-NY-NYIS": "NYIS",  # New York Independent System Operator
    # "US-SE-AEC": "AEC",  # Powersouth Energy Cooperative, decomissioned merged with US-SE-SOCO
    # Though it is unclear which BA took over AEC.
    "US-SE-SEPA": "SEPA",  # Southeastern Power Administration
    "US-SE-SOCO": "SOCO",  # Southern Company Services, Inc. - Trans
    "US-SW-AZPS": "AZPS",  # Arizona Public Service Company
    "US-SW-DEAA": "DEAA",  # Arlington Valley, LLC, integrated with US-SW-SRP
    "US-SW-EPE": "EPE",  # El Paso Electric Company
    "US-SW-GRIF": "GRIF",  # Griffith Energy, Llc, integrated with US-SW-WALC
    #   "US-SW-GRMA": "GRMA",  # Gila River Power, Llc Decommissioned,
    #  The only gas power plant is owned by US-SW-SRP but there's a PPA with US-SW-AZPS, so it was merged with
    # US-SW-AZPS https://www.power-technology.com/marketdata/gila-river-power-station-us/
    "US-SW-HGMA": "HGMA",  # New Harquahala Generating Company, Llc - Hgba, integrated with US-SW-SRP
    "US-SW-PNM": "PNM",  # Public Service Company Of New Mexico
    "US-SW-SRP": "SRP",  # Salt River Project
    "US-SW-TEPC": "TEPC",  # Tucson Electric Power Company
    "US-SW-WALC": "WALC",  # Western Area Power Administration - Desert Southwest Region
    "US-TEN-TVA": "TVA",  # Tennessee Valley Authority
    "US-TEX-ERCO": "ERCO",  # Electric Reliability Council Of Texas, Inc.
}

EXCHANGES = {
    # Exchanges to non-US BAs
    "MX-BC->US-CAL-CISO": "&facets[fromba][]=CISO&facets[toba][]=CFE",  # Unable to verify if MX-BC is correct
    "CA-SK->US-CENT-SWPP": "&facets[fromba][]=SWPP&facets[toba][]=SPC",
    "CA-MB->US-MIDW-MISO": "&facets[fromba][]=MISO&facets[toba][]=MHEB",
    "CA-ON->US-MIDW-MISO": "&facets[fromba][]=MISO&facets[toba][]=IESO",
    "CA-QC->US-NE-ISNE": "&facets[fromba][]=ISNE&facets[toba][]=HQT",
    "CA-NB->US-NE-ISNE": "&facets[fromba][]=ISNE&facets[toba][]=NBSO",
    "CA-BC->US-NW-BPAT": "&facets[fromba][]=BPAT&facets[toba][]=BCHA",
    "CA-AB->US-NW-NWMT": "&facets[fromba][]=NWMT&facets[toba][]=AESO",
    "CA-QC->US-NY-NYIS": "&facets[fromba][]=NYIS&facets[toba][]=HQT",
    "CA-ON->US-NY-NYIS": "&facets[fromba][]=NYIS&facets[toba][]=IESO",
    "MX-NE->US-TEX-ERCO": "&facets[fromba][]=ERCO&facets[toba][]=CEN",  # Unable to verify if MX-NE is correct
    "MX-NO->US-TEX-ERCO": "&facets[fromba][]=ERCO&facets[toba][]=CFE",  # Unable to verify if MX-NO is correct
    # Exchanges to other US balancing authorities
    "US-CAL-BANC->US-NW-BPAT": "&facets[fromba][]=BANC&facets[toba][]=BPAT",
    "US-CAL-BANC->US-CAL-CISO": "&facets[fromba][]=BANC&facets[toba][]=CISO",
    "US-CAL-BANC->US-CAL-TIDC": "&facets[fromba][]=BANC&facets[toba][]=TIDC",
    "US-CAL-CISO->US-SW-AZPS": "&facets[fromba][]=CISO&facets[toba][]=AZPS",
    "US-CAL-CISO->US-NW-BPAT": "&facets[fromba][]=CISO&facets[toba][]=BPAT",
    "US-CAL-CISO->US-CAL-IID": "&facets[fromba][]=CISO&facets[toba][]=IID",
    "US-CAL-CISO->US-CAL-LDWP": "&facets[fromba][]=CISO&facets[toba][]=LDWP",
    "US-CAL-CISO->US-NW-NEVP": "&facets[fromba][]=CISO&facets[toba][]=NEVP",
    "US-CAL-CISO->US-NW-PACW": "&facets[fromba][]=CISO&facets[toba][]=PACW",
    "US-CAL-CISO->US-SW-SRP": "&facets[fromba][]=CISO&facets[toba][]=SRP",
    "US-CAL-CISO->US-CAL-TIDC": "&facets[fromba][]=CISO&facets[toba][]=TIDC",
    "US-CAL-CISO->US-SW-WALC": "&facets[fromba][]=CISO&facets[toba][]=WALC",
    "US-CAL-IID->US-SW-AZPS": "&facets[fromba][]=IID&facets[toba][]=AZPS",
    "US-CAL-IID->US-SW-WALC": "&facets[fromba][]=IID&facets[toba][]=WALC",
    "US-CAL-LDWP->US-SW-AZPS": "&facets[fromba][]=LDWP&facets[toba][]=AZPS",
    "US-CAL-LDWP->US-NW-BPAT": "&facets[fromba][]=LDWP&facets[toba][]=BPAT",
    "US-CAL-LDWP->US-NW-NEVP": "&facets[fromba][]=LDWP&facets[toba][]=NEVP",
    "US-CAL-LDWP->US-NW-PACE": "&facets[fromba][]=LDWP&facets[toba][]=PACE",
    "US-CAL-LDWP->US-SW-WALC": "&facets[fromba][]=LDWP&facets[toba][]=WALC",
    "US-CAR-CPLE->US-CAR-YAD": "&facets[fromba][]=CPLE&facets[toba][]=YAD",
    "US-CAR-CPLE->US-CAR-DUK": "&facets[fromba][]=CPLE&facets[toba][]=DUK",
    "US-CAR-CPLE->US-MIDA-PJM": "&facets[fromba][]=CPLE&facets[toba][]=PJM",
    "US-CAR-CPLE->US-CAR-SCEG": "&facets[fromba][]=CPLE&facets[toba][]=SCEG",
    "US-CAR-CPLE->US-CAR-SC": "&facets[fromba][]=CPLE&facets[toba][]=SC",
    "US-CAR-CPLW->US-CAR-DUK": "&facets[fromba][]=CPLW&facets[toba][]=DUK",
    "US-CAR-CPLW->US-MIDA-PJM": "&facets[fromba][]=CPLW&facets[toba][]=PJM",
    "US-CAR-CPLW->US-TEN-TVA": "&facets[fromba][]=CPLW&facets[toba][]=TVA",
    "US-CAR-DUK->US-CAR-YAD": "&facets[fromba][]=DUK&facets[toba][]=YAD",
    "US-CAR-DUK->US-MIDA-PJM": "&facets[fromba][]=DUK&facets[toba][]=PJM",
    "US-CAR-DUK->US-CAR-SCEG": "&facets[fromba][]=DUK&facets[toba][]=SCEG",
    "US-CAR-DUK->US-CAR-SC": "&facets[fromba][]=DUK&facets[toba][]=SC",
    "US-CAR-DUK->US-SE-SEPA": "&facets[fromba][]=DUK&facets[toba][]=SEPA",
    "US-CAR-DUK->US-SE-SOCO": "&facets[fromba][]=DUK&facets[toba][]=SOCO",
    "US-CAR-DUK->US-TEN-TVA": "&facets[fromba][]=DUK&facets[toba][]=TVA",
    "US-CAR-SC->US-CAR-SCEG": "&facets[fromba][]=SC&facets[toba][]=SCEG",
    "US-CAR-SC->US-SE-SEPA": "&facets[fromba][]=SC&facets[toba][]=SEPA",
    "US-CAR-SC->US-SE-SOCO": "&facets[fromba][]=SC&facets[toba][]=SOCO",
    "US-CAR-SCEG->US-SE-SEPA": "&facets[fromba][]=SCEG&facets[toba][]=SEPA",
    "US-CAR-SCEG->US-SE-SOCO": "&facets[fromba][]=SCEG&facets[toba][]=SOCO",
    "US-CENT-SPA->US-MIDW-AECI": "&facets[fromba][]=SPA&facets[toba][]=AECI",
    "US-CENT-SPA->US-MIDW-MISO": "&facets[fromba][]=SPA&facets[toba][]=MISO",
    "US-CENT-SPA->US-CENT-SWPP": "&facets[fromba][]=SPA&facets[toba][]=SWPP",
    "US-CENT-SWPP->US-MIDW-AECI": "&facets[fromba][]=SWPP&facets[toba][]=AECI",
    "US-CENT-SWPP->US-SW-EPE": "&facets[fromba][]=SWPP&facets[toba][]=EPE",
    "US-CENT-SWPP->US-TEX-ERCO": "&facets[fromba][]=SWPP&facets[toba][]=ERCO",
    "US-CENT-SWPP->US-MIDW-MISO": "&facets[fromba][]=SWPP&facets[toba][]=MISO",
    "US-CENT-SWPP->US-NW-PSCO": "&facets[fromba][]=SWPP&facets[toba][]=PSCO",
    "US-CENT-SWPP->US-SW-PNM": "&facets[fromba][]=SWPP&facets[toba][]=PNM",
    "US-CENT-SWPP->US-NW-WACM": "&facets[fromba][]=SWPP&facets[toba][]=WACM",
    "US-CENT-SWPP->US-NW-WAUW": "&facets[fromba][]=SWPP&facets[toba][]=WAUW",
    "US-FLA-FMPP->US-FLA-FPC": "&facets[fromba][]=FMPP&facets[toba][]=FPC",
    "US-FLA-FMPP->US-FLA-FPL": "&facets[fromba][]=FMPP&facets[toba][]=FPL",
    "US-FLA-FMPP->US-FLA-JEA": "&facets[fromba][]=FMPP&facets[toba][]=JEA",
    "US-FLA-FMPP->US-FLA-TEC": "&facets[fromba][]=FMPP&facets[toba][]=TEC",
    "US-FLA-FPC->US-FLA-TAL": "&facets[fromba][]=FPC&facets[toba][]=TAL",
    "US-FLA-FPC->US-FLA-FPL": "&facets[fromba][]=FPC&facets[toba][]=FPL",
    "US-FLA-FPC->US-FLA-GVL": "&facets[fromba][]=FPC&facets[toba][]=GVL",
    "US-FLA-FPC->US-FLA-SEC": "&facets[fromba][]=FPC&facets[toba][]=SEC",
    "US-FLA-FPC->US-SE-SOCO": "&facets[fromba][]=FPC&facets[toba][]=SOCO",
    "US-FLA-FPC->US-FLA-TEC": "&facets[fromba][]=FPC&facets[toba][]=TEC",
    #    "US-FLA-FPC->US-FLA-NSB": "&facets[fromba][]=FPC&facets[toba][]=NSB", decomissioned NSB zone
    "US-FLA-FPL->US-FLA-HST": "&facets[fromba][]=FPL&facets[toba][]=HST",
    "US-FLA-FPL->US-FLA-GVL": "&facets[fromba][]=FPL&facets[toba][]=GVL",
    "US-FLA-FPL->US-FLA-JEA": "&facets[fromba][]=FPL&facets[toba][]=JEA",
    "US-FLA-FPL->US-FLA-SEC": "&facets[fromba][]=FPL&facets[toba][]=SEC",
    "US-FLA-FPL->US-SE-SOCO": "&facets[fromba][]=FPL&facets[toba][]=SOCO",
    "US-FLA-FPL->US-FLA-TEC": "&facets[fromba][]=FPL&facets[toba][]=TEC",
    #    "US-FLA-FPL->US-FLA-NSB": "&facets[fromba][]=FPL&facets[toba][]=NSB", decomissioned NSB zone
    "US-FLA-JEA->US-FLA-SEC": "&facets[fromba][]=JEA&facets[toba][]=SEC",
    "US-FLA-SEC->US-FLA-TEC": "&facets[fromba][]=SEC&facets[toba][]=TEC",
    "US-FLA-TAL->US-SE-SOCO": "&facets[fromba][]=TAL&facets[toba][]=SOCO",
    "US-MIDA-PJM->US-MIDW-LGEE": "&facets[fromba][]=PJM&facets[toba][]=LGEE",
    "US-MIDA-PJM->US-MIDW-MISO": "&facets[fromba][]=PJM&facets[toba][]=MISO",
    "US-MIDA-PJM->US-NY-NYIS": "&facets[fromba][]=PJM&facets[toba][]=NYIS",
    "US-MIDA-PJM->US-TEN-TVA": "&facets[fromba][]=PJM&facets[toba][]=TVA",
    "US-MIDW-AECI->US-MIDW-MISO": "&facets[fromba][]=AECI&facets[toba][]=MISO",
    "US-MIDW-AECI->US-TEN-TVA": "&facets[fromba][]=AECI&facets[toba][]=TVA",
    # "US-MIDW-GLHB->US-MIDW-LGEE": "&facets[fromba][]=GLHB&facets[toba][]=LGEE", US-MIDW-GLHB decommissioned no more powerplant
    # "US-MIDW-GLHB->US-MIDW-MISO": "&facets[fromba][]=GLHB&facets[toba][]=MISO", US-MIDW-GLHB decommissioned no more powerplant
    # "US-MIDW-GLHB->US-TEN-TVA": "&facets[fromba][]=EEI&facets[toba][]=TVA", US-MIDW-GLHB decommissioned no more powerplant
    "US-MIDW-LGEE->US-MIDW-MISO": "&facets[fromba][]=LGEE&facets[toba][]=MISO",
    "US-MIDW-LGEE->US-TEN-TVA": "&facets[fromba][]=LGEE&facets[toba][]=TVA",
    "US-MIDW-MISO->US-SE-AEC": "&facets[fromba][]=MISO&facets[toba][]=AEC",
    "US-MIDW-MISO->US-SE-SOCO": "&facets[fromba][]=MISO&facets[toba][]=SOCO",
    "US-MIDW-MISO->US-TEN-TVA": "&facets[fromba][]=MISO&facets[toba][]=TVA",
    "US-NE-ISNE->US-NY-NYIS": "&facets[fromba][]=ISNE&facets[toba][]=NYIS",
    "US-NW-AVA->US-NW-BPAT": "&facets[fromba][]=AVA&facets[toba][]=BPAT",
    "US-NW-AVA->US-NW-IPCO": "&facets[fromba][]=AVA&facets[toba][]=IPCO",
    "US-NW-AVA->US-NW-NWMT": "&facets[fromba][]=AVA&facets[toba][]=NWMT",
    "US-NW-AVA->US-NW-PACW": "&facets[fromba][]=AVA&facets[toba][]=PACW",
    "US-NW-AVA->US-NW-CHPD": "&facets[fromba][]=AVA&facets[toba][]=CHPD",
    "US-NW-AVA->US-NW-GCPD": "&facets[fromba][]=AVA&facets[toba][]=GCPD",
    "US-NW-BPAT->US-NW-TPWR": "&facets[fromba][]=BPAT&facets[toba][]=TPWR",
    "US-NW-BPAT->US-NW-GRID": "&facets[fromba][]=BPAT&facets[toba][]=GRID",
    "US-NW-BPAT->US-NW-IPCO": "&facets[fromba][]=BPAT&facets[toba][]=IPCO",
    "US-NW-BPAT->US-NW-NEVP": "&facets[fromba][]=BPAT&facets[toba][]=NEVP",
    "US-NW-BPAT->US-NW-NWMT": "&facets[fromba][]=BPAT&facets[toba][]=NWMT",
    "US-NW-BPAT->US-NW-DOPD": "&facets[fromba][]=BPAT&facets[toba][]=DOPD",
    "US-NW-BPAT->US-NW-PACW": "&facets[fromba][]=BPAT&facets[toba][]=PACW",
    "US-NW-BPAT->US-NW-PGE": "&facets[fromba][]=BPAT&facets[toba][]=PGE",
    "US-NW-BPAT->US-NW-CHPD": "&facets[fromba][]=BPAT&facets[toba][]=CHPD",
    "US-NW-BPAT->US-NW-GCPD": "&facets[fromba][]=BPAT&facets[toba][]=GCPD",
    "US-NW-BPAT->US-NW-PSEI": "&facets[fromba][]=BPAT&facets[toba][]=PSEI",
    "US-NW-BPAT->US-NW-SCL": "&facets[fromba][]=BPAT&facets[toba][]=SCL",
    "US-NW-CHPD->US-NW-DOPD": "&facets[fromba][]=CHPD&facets[toba][]=DOPD",
    "US-NW-CHPD->US-NW-PSEI": "&facets[fromba][]=CHPD&facets[toba][]=PSEI",
    "US-NW-GCPD->US-NW-PACW": "&facets[fromba][]=GCPD&facets[toba][]=PACW",
    "US-NW-GCPD->US-NW-PSEI": "&facets[fromba][]=GCPD&facets[toba][]=PSEI",
    #    "US-NW-GWA->US-NW-NWMT": "&facets[fromba][]=GWA&facets[toba][]=NWMT", integrated directly with US-NW-NWMT
    "US-NW-IPCO->US-NW-NEVP": "&facets[fromba][]=IPCO&facets[toba][]=NEVP",
    "US-NW-IPCO->US-NW-NWMT": "&facets[fromba][]=IPCO&facets[toba][]=NWMT",
    "US-NW-IPCO->US-NW-PACE": "&facets[fromba][]=IPCO&facets[toba][]=PACE",
    "US-NW-IPCO->US-NW-PACW": "&facets[fromba][]=IPCO&facets[toba][]=PACW",
    "US-NW-NEVP->US-NW-PACE": "&facets[fromba][]=NEVP&facets[toba][]=PACE",
    "US-NW-NEVP->US-SW-WALC": "&facets[fromba][]=NEVP&facets[toba][]=WALC",
    #    "US-NW-NWMT->US-NW-WWA": "&facets[fromba][]=NWMT&facets[toba][]=WWA", intergrated directly with US-NW-NWMT
    "US-NW-NWMT->US-NW-PACE": "&facets[fromba][]=NWMT&facets[toba][]=PACE",
    "US-NW-NWMT->US-NW-WAUW": "&facets[fromba][]=NWMT&facets[toba][]=WAUW",
    "US-NW-PACE->US-SW-AZPS": "&facets[fromba][]=PACE&facets[toba][]=AZPS",
    "US-NW-PACE->US-NW-PACW": "&facets[fromba][]=PACE&facets[toba][]=PACW",
    "US-NW-PACE->US-NW-WACM": "&facets[fromba][]=PACE&facets[toba][]=WACM",
    "US-NW-PACW->US-NW-PGE": "&facets[fromba][]=PACW&facets[toba][]=PGE",
    "US-NW-PSCO->US-SW-PNM": "&facets[fromba][]=PSCO&facets[toba][]=PNM",
    "US-NW-PSCO->US-NW-WACM": "&facets[fromba][]=PSCO&facets[toba][]=WACM",
    "US-NW-PSEI->US-NW-TPWR": "&facets[fromba][]=PSEI&facets[toba][]=TPWR",
    "US-NW-PSEI->US-NW-SCL": "&facets[fromba][]=PSEI&facets[toba][]=SCL",
    "US-NW-WACM->US-SW-AZPS": "&facets[fromba][]=WACM&facets[toba][]=AZPS",
    "US-NW-WACM->US-SW-PNM": "&facets[fromba][]=WACM&facets[toba][]=PNM",
    "US-NW-WACM->US-SW-WALC": "&facets[fromba][]=WACM&facets[toba][]=WALC",
    "US-NW-WACM->US-NW-WAUW": "&facets[fromba][]=WACM&facets[toba][]=WAUW",
    # "US-SE-AEC->US-SE-SOCO": "&facets[fromba][]=AEC&facets[toba][]=SOCO", Decommisioned BA
    "US-SE-SEPA->US-SE-SOCO": "&facets[fromba][]=SEPA&facets[toba][]=SOCO",
    "US-SE-SOCO->US-TEN-TVA": "&facets[fromba][]=SOCO&facets[toba][]=TVA",
    #    "US-SW-AZPS->US-SW-GRMA": "&facets[fromba][]=AZPS&facets[toba][]=GRMA", Decommissioned
    "US-SW-AZPS->US-SW-PNM": "&facets[fromba][]=AZPS&facets[toba][]=PNM",
    "US-SW-AZPS->US-SW-SRP": "&facets[fromba][]=AZPS&facets[toba][]=SRP",
    "US-SW-AZPS->US-SW-TEPC": "&facets[fromba][]=AZPS&facets[toba][]=TEPC",
    "US-SW-AZPS->US-SW-WALC": "&facets[fromba][]=AZPS&facets[toba][]=WALC",
    "US-SW-EPE->US-SW-PNM": "&facets[fromba][]=EPE&facets[toba][]=PNM",
    "US-SW-EPE->US-SW-TEPC": "&facets[fromba][]=EPE&facets[toba][]=TEPC",
    #    "US-SW-GRIF->US-SW-WALC": "&facets[fromba][]=GRIF&facets[toba][]=WALC", directly integrated in US-WALC
    #    "US-SW-HGMA->US-SW-SRP": "&facets[fromba][]=HGMA&facets[toba][]=SRP", directly integrated in US-SW-SRP
    "US-SW-PNM->US-SW-TEPC": "&facets[fromba][]=PNM&facets[toba][]=TEPC",
    "US-SW-PNM->US-SW-SRP": "&facets[fromba][]=SRP&facets[toba][]=PNM",
    "US-SW-SRP->US-SW-TEPC": "&facets[fromba][]=SRP&facets[toba][]=TEPC",
    "US-SW-SRP->US-SW-WALC": "&facets[fromba][]=SRP&facets[toba][]=WALC",
    "US-SW-TEPC->US-SW-WALC": "&facets[fromba][]=TEPC&facets[toba][]=WALC",
}
# Some zones transfer all or part of their productions to another zone.
# To avoid having multiple small production zones with no consumption,
# their production is directly integrated into supplied zones according
# to the supplied percentage.

SC_VIRGIL_OWNERSHIP = 0.3333333

PRODUCTION_ZONES_TRANSFERS = {
    "US-SW-SRP": {"all": {"US-SW-DEAA": 1.0, "US-SW-HGMA": 1.0}},
    "US-NW-NWMT": {"all": {"US-NW-GWA": 1.0, "US-NW-WWA": 1.0}},
    "US-SW-WALC": {"all": {"US-SW-GRIF": 1.0}},
    "US-NW-PACW": {"gas": {"US-NW-AVRN": 1.0}},
    "US-NW-BPAT": {
        "wind": {"US-NW-AVRN": 1.0},
    },
    "US-CAR-SC": {"nuclear": {"US-CAR-SCEG": SC_VIRGIL_OWNERSHIP}},
}

TYPES = {
    # 'biomass': 'BM',  # not currently supported
    "coal": "COL",
    "gas": "NG",
    "hydro": "WAT",
    "nuclear": "NUC",
    "oil": "OIL",
    "unknown": "OTH",
    "solar": "SUN",
    "wind": "WND",
}

BASE_URL = "https://api.eia.gov/v2/electricity/rto"

PRODUCTION = (
    f"{BASE_URL}/region-data/data/"
    "?data[]=value&facets[respondent][]={}&facets[type][]=NG&frequency=hourly"
)
CONSUMPTION = (
    f"{BASE_URL}/region-data/data/"
    "?data[]=value&facets[respondent][]={}&facets[type][]=D&frequency=hourly"
)
CONSUMPTION_FORECAST = (
    f"{BASE_URL}/region-data/data/"
    "?data[]=value&facets[respondent][]={}&facets[type][]=DF&frequency=hourly"
)
PRODUCTION_MIX = (
    f"{BASE_URL}/fuel-type-data/data/"
    "?data[]=value&facets[respondent][]={}&facets[fueltype][]={}&frequency=hourly"
)
EXCHANGE = f"{BASE_URL}/interchange-data/data/" "?data[]=value{}&frequency=hourly"
