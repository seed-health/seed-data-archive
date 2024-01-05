import tableauserverclient as TSC
from tableau_api_lib import TableauServerConnection

# token_name = os.getenv('token_name')
# token_secret = os.getenv('token_secret')
# site_name = os.getenv('site_name')
# server_url = os.getenv('server_url')
# wb_filter = os.getenv('wb_filter')
# view_filter = os.getenv('view_filter')
# view_id = os.getenv('view_id')
    
token_name = 'tableau_etl'
token_secret = 'RNrSb9KOQRWmMHZysJBLAA==:IeRVUnV9pKCfiAmljQLQDgYyNPMyycd9'
site_name = 'seedhealth'
server_url = 'https://us-west-2b.online.tableau.com/'
# wb_filter = config.get('Params','wb_filter')
# view_filter = config.get('Params','view_filter')
# view_id = config.get('Params', 'view_id')

# sender = os.getenv('sender')
# recipient = os.getenv('recipient')
# subject = os.getenv('subject')
# smtpUser = os.getenv('smtpUser')
# smtpPassword = os.getenv('smtpPassword')

# sender = config.get('Email', 'sender')
# recipient = config.get('Email', 'recipient')
# subject = config.get('Email', 'subject')
# smtpUser = config.get('Email', 'smtpUser')
# smtpPassword = config.get('Email', 'smtpPassword')

def sign_in(token_name, token_secret, site_name, server_url):
    tableau_auth = TSC.PersonalAccessTokenAuth(token_name, token_secret, site_name)
    server = TSC.Server(server_url)
    return tableau_auth, server

def list_all_workbooks(tableau_auth, server):
    with server.auth.sign_in(tableau_auth):
        all_wb, pagination_item = server.workbooks.get()
        # logger.info(f'There are {str(pagination_item.total_available)} workbooks on site: \n')
        for wb in all_wb:
            return wb.id, wb.name

def get_view(tableau_auth, wb_filter, view_filter, view_id, server):
    with server.auth.sign_in(tableau_auth):
        all_workbooks = server.workbooks.filter(name=wb_filter)
        for workbook in all_workbooks:
            server.workbooks.populate_views(workbook)

            view_item = server.views.filter(name=view_filter)
            # print(view_item)
            # logging.log(f'The views for {workbook.name}: \n')
            # print([(view.name, view.id) for view in workbook.views])
        view_item = server.views.get_by_id(view_id)
        # logger.info(view_item.name)
        return view_item

tableau_auth, server = sign_in(token_name, token_secret, site_name, server_url)
server.version = '3.6'

list_all_workbooks(tableau_auth, server)
# view_item = get_view(tableau_auth, wb_filter, view_filter, view_id, server)


# Tableau Server connection details
# tableau_server_config = {
#   'test': {
#     'server': 'https://us-west-2b.online.tableau.com',
#     'api_version': '3.21',
#     'username' : 'salma@seed.com',
#     'password' : 'Malik2016?',
#     'site' : 'seedhealth',
#     'site_url': 'https://us-west-2b.online.tableau.com/#/site/seedhealth'
#   }
# }
# server = 'https://us-west-2b.online.tableau.com'
# username = 'salma@seed.com'
# password = 'Malik2016?'
# site = 'seedhealth',


# # Connect to Tableau Server
# connection = TableauServerConnection(tableau_server_config, 'test')
# connection.sign_in()

# Get projects and workbooks
# all_projects, pagination_item = get_projects(connection)
# for project in all_projects:
#     print(f"Project: {project.name}")
#     workbooks, pagination_item = get_workbooks(connection, project_id=project.id)
#     for workbook in workbooks:
#         print(f"  Workbook: {workbook.name}, ID: {workbook.id}")

# # Sign out from Tableau Server
# connection.sign_out()
