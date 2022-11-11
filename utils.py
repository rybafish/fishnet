#
# The simplified version of RybaFish utils toolbox, 2022-10-13
# 

import sys, os, time

from datetime import datetime

import locale

from yaml import safe_load, dump, YAMLError

from profiler import profiler

import re

config = {}

timers = []

#globals:
localeCfg = None
thousands_separator = ''
decimal_point = '.'

cfg_logmode = 'both'
cfg_loglevel = 3

configStats = {}

def loadConfig(silent=False):

    global config
    
    script = sys.argv[0]
    path, file = os.path.split(script)
    
    cfgFile = os.path.join(path, 'config.yaml')

    config.clear()

    try: 
        f = open(cfgFile, 'r')
        config = safe_load(f)
        f.close()
                    
    except:
        if not silent:
            log('no config file? <-')
            
        config = {}
        
        return False

    return True


@profiler
def cfg(param, default=None, mandatory=False):

    global config
    global configStats
    
    if configStats:
        if param in configStats:
            configStats[param] += 1
        else:
            configStats[param] = 1
    
    if param in config:
        return config[param]
    else:
        if mandatory == False:
            return default    
        else:
            raise Exception(f'The parameter {param} is mandatory.')

    
loadConfig(silent=True) # try to silently init config...

@profiler
def log(s, loglevel = 3, nots = False, nonl = False):
    '''
        log the stuff one way or another...
    '''
    
    if cfg_loglevel < loglevel:
        return

    if not nots:
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' '
    else:
        ts = ''
    
    if cfg_logmode == 'screen' or cfg_logmode == 'duplicate':
        print('[l]', s)
        
    if nonl:
        nl = ''
    else:
        nl = '\n'

    if cfg_logmode != 'screen':
    
        f = open('.log', 'a')
    
        try:
            f.write(ts + str(s) + nl)

        except Exception as e:
            f.write(ts + str(e) + nl)
    
        f.close()

        
@profiler
def initLocale():
    '''
        sets the locale based configuration
        
        sets thousands_separator and decimal_point globals
    '''
    
    global localeCfg
    global thousands_separator
    global decimal_point

    localeCfg = cfg('locale', '')
    if localeCfg != '':
        log(f'Locale setting to: {localeCfg}')
        try:
            locale.setlocale(locale.LC_ALL, localeCfg)
        except Exception as e:
            localeCfg = ''
            log(f'[!] Locale error: {str(e)}, {localeCfg}', 2)
            log(f'[!] List of supported locales: {str(list(locale.locale_alias.keys()))}', 2)
                
    # just once now, 2022-10-03
        
    if localeCfg == '':
        locale.setlocale(locale.LC_ALL, localeCfg)
                
    thousands_separator = cfg('thousandsSeparator') or locale.localeconv()['thousands_sep']
    decimal_point = cfg('decimalPoint') or locale.localeconv()['decimal_point']
    
    log(f'Locale thousands separator is: [{thousands_separator}], decimal point: [{decimal_point}]', 5)
    
def initGlobalSettings():
    global configStats
    global cfg_logmode
    global cfg_loglevel
    
    if cfg('dev'):
        configStats['dummy'] = 0
        
    cfg_logmode = cfg('logmode', 'duplicate')
    cfg_loglevel = cfg('loglevel', 3)
    
    initLocale()
    
initGlobalSettings()
    
def intToStr(x, grp = True):
    #only integers, only >= 0
    
    global thousands_separator
    
    if grp == False:
        return str(x)
    
    if x < 1000:
        return str(x)
    else:
        x, r = divmod(x, 1000)
        return f'{intToStr(x)}{thousands_separator}{r:03}'

def numberToStr(x, grp=True, digits=None):

    global decimal_point
    
    #bkp = x
    
    if x < 0:
        x = -x
        sign = '-'
    else:
        sign = ''
    
    fr = x%1
    x = int(x)
    
    #print(f'{bkp} ==> sign:{sign} int: {x} dec: {fr}, {grp=}, {digits=}')
    
    if fr:
        if digits:
            frs = decimal_point + f'{fr:.{digits}f}'[2:]        # duplicated formula, but a bit different case
        elif digits == 0:
            frs = ''
        else:
            frs = decimal_point + f'{fr:.6f}'.rstrip('0').rstrip(decimal_point)[2:]
    else:
        if digits:
            frs = decimal_point + f'{fr:.{digits}f}'[2:]        # duplicated formula, but a bit different case
        else:
            frs = ''
                
    return sign + intToStr(x, grp) + frs


def fileBytes(size):
    if size >= 1024*1024*1024:
        return numberToStr(round(size/(1024*1024*1024), 1)) + 'gb'
    elif size >= 1024*1024:
        return numberToStr(round(size/(1024*1024), 1)) + 'mb'
    elif size >= 1024*10:
        return numberToStr(round(size/1024, 1)) + 'kb'
    else:
        return numberToStr(size) + 'b'
    