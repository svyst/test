import logging.config
import os
from configparser import ConfigParser, ExtendedInterpolation
from os import path

from source.scrapping import get_forms_df
from source.emails import Mail
from source.tools import delete_temp, make_loger, get_output_map
from source.excel import Input, Output


def initialisation():
    try:
        config = ConfigParser(interpolation=ExtendedInterpolation(), delimiters=(':'))
        config.read_file(open(path.join('settings', 'config.cfg')))
    except Exception:
        logger.error('Config is not declared.')
        raise Exception
    logger.info(config.get('info', 'config_read'))
    if not os.path.exists(config.get('path', 'mapping')):
        logger.error(config.get('error', 'output_columns_path').format(config.get('path', 'mapping')))
        raise Exception
    try:
        output_columns_map = get_output_map(config)
    except Exception:
        logger.error(config.get('error', 'output_columns').format(config.get('path', 'mapping')))
        raise Exception
    delete_temp(config)
    return config, output_columns_map


def main_steps(config, output_columns_map):
    while True:
        try:
            message = Mail(config)
            message.save_earlier_mail_attachment()
            input = Input(config, file_path=message.saved_attachment)
            search_names = input.parse_input(message)
            break
        except Exception as exception:
            delete_temp(config)
            if '*Failed_message' not in exception.args:
                raise exception  # unexpected error

    if len(search_names) > 0:
        logger.info(config.get('info', 'scrapping'))
        try:
            scrap_result = get_forms_df(config, search_names, output_columns_map)
        except Exception:
            logger.error(config.get('error', 'fpds_site'))
            message.send_fail_to_admin(letter=3)
            raise Exception('*Handled_error')
        input.add_status_column(scrap_result['status_res'])
        if not scrap_result['output_df'].empty:
            output = Output(config)
            output.write(scrap_result['output_df'])
            message.success_reply(output_file=output.path, no_processed=input.has_non_processed)
            return
    message.success_reply(output_file=None, no_processed=input.has_non_processed)


if __name__ == "__main__":
    make_loger()
    logger = logging.getLogger('mainFPDS')
    logger.info('FPDS bot execution started')
    try:
        config, output_columns_map = initialisation()
        main_steps(config, output_columns_map)
        delete_temp(config)
    except Exception as ex:
        if '*Handled_error' in ex.args:
            try:
                delete_temp(config)
                logger.error(config.get('error', 'unexpected'), exc_info=True)
                unexpected_message = Mail(config)
                unexpected_message.send_fail_to_admin(letter=4)
            except NameError:
                pass
            except Exception:
                logger.error(config.get('error', 'unexpected'), exc_info=True)
                delete_temp(config)
    logger.info('FPDS bot execution ended.')
