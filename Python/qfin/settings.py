from .config_loader import ConfigLoader

# Production Related
run_type = 'PROD'

# Auxiliary variables 
log_txt = ''



# DataBase Config
class DB_Config(object):
    crypto_crncy_str = "mysql+mysqldb://username:password@localhost/crypto_crncy?unix_socket=/tmp/mysql.sock"


# AWS Config
class AWS_Config(object):
    EMAIL_HOST          = 'email-smtp.us-east-1.amazonaws.com'
    EMAIL_HOST_USER     = 'AKIAJNO3QXF35G5LP75A'
    EMAIL_HOST_PASSWORD = 'AiuhyyfR9LFKaM1N0lsE9lN2m+18cj82ewoQNU6rusei'
    EMAIL_PORT          = 587

    Sender     = 'yzhao0527@gmail.com'
    Receipient = 'yzhao0527@gmail.com'


# GDAX
class GDAX_Config(object):
    key    = "xxx"
    seckey = "xxx"
    passwd = "xxx"

# Make sure you verify the email through AWS SES before adding it to this list
# Otherwise the program will fail
debug_email_receipients = None
prod_email_receipients  = None

# Workspaces
workspace_cryptoccy = None

# server authentification
svc_username = "ubuntu"
svc_pem_file = None



def load_config(config):
    assert isinstance(config, ConfigLoader)

    global debug_email_receipients
    debug_email_receipients = config.get('debug_email_receipients')

    global prod_email_receipients
    prod_email_receipients = config.get('prod_email_receipients')

    global workspace_cryptoccy
    workspace_cryptoccy = config.get('workspace_cryptoccy')

    GDAX_Config.key    = config.get('GDAX_key'   , GDAX_Config.key   )
    GDAX_Config.seckey = config.get('GDAX_seckey', GDAX_Config.seckey)
    GDAX_Config.passwd = config.get('GDAX_passwd', GDAX_Config.passwd)

    AWS_Config.EMAIL_HOST_USER     = config.get('AWS_EMAIL_HOST_USER'    , AWS_Config.EMAIL_HOST_USER    )
    AWS_Config.EMAIL_HOST_PASSWORD = config.get('AWS_EMAIL_HOST_PASSWORD', AWS_Config.EMAIL_HOST_PASSWORD)

    global svc_pem_file
    svc_pem_file = config.get('svc_pem_file')

    global run_type
    run_type = config.get('run_type', 'PROD')
    assert run_type in {'SIM', 'PROD'}, "Invalid run_type: %s" % run_type
