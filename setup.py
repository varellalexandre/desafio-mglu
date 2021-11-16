from preparacao_dos_dados import main_pipeline_base_de_dados
import logging


logger = logging.getLogger(__name__)

def setup():
    logger.info("Iniciando base de dados")
    main_pipeline_base_de_dados('base_de_dados/raw')


if __name__ == '__main__':
    setup()
