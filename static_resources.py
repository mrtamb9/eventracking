import data_reader,resources
#CATMAP = event_dao.get_all_categories()
def get_domains_ids():
    DOMAIN_TO_ID = dict()
    ID_TO_DOMAIN = dict()
    domains = data_reader.get_domains(resources.DOMAIN_PATH)
    for domain in domains:
        new_id = len(DOMAIN_TO_ID)
        DOMAIN_TO_ID[domain] = new_id
        ID_TO_DOMAIN[new_id] = domain
    return (DOMAIN_TO_ID,ID_TO_DOMAIN)
#(DOMAIN_TO_ID,ID_TO_DOMAIN) = get_domains_ids() 
    