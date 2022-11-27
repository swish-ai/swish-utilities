import requests
from urllib.parse import urlparse, quote_plus

def get_next_location(resp):
    path = None
    if len(resp.history) > 0:
        path = urlparse(resp.history[0].url).path
    return path

def fetch_server_root(url:str):
    response =  requests.get(url)
    headers = response.headers
    pairs = headers['Set-Cookie'].split(';')
    cookies = {kv[0].split(',')[-1].strip(): kv[1] for kv in [p.split('=') for p in pairs] if len(kv) > 1}
    return cookies


def login(base_url:str, page_cookies, username, password):
    password = quote_plus(password)
    cookies = {
    'JSESSIONID': page_cookies["JSESSIONID"],
    'glide_user_route': page_cookies["glide_user_route"]
    }

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': f'{base_url}',
        'Referer': f'{base_url}/welcome.do',
        'Sec-Fetch-Dest': 'iframe',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
    }

    data = f'user_name={username}&user_password={password}&ni.nolog.user_password=true&ni.noecho.user_name=true&ni.noecho.user_password=true&screensize=1440x900&sys_action=sysverb_login&sysparm_login_url=welcome.do&not_important='

    response = requests.post(f'{base_url}/login.do', cookies=cookies, headers=headers, data=data, allow_redirects=False)
    # next_location = get_next_location(response)
    headers = response.headers
    next_location = headers['Location']

    pairs = headers['Set-Cookie'].split(';')
    cookies = {kv[0].split(',')[-1].strip(): kv[1] for kv in [p.split('=') for p in pairs] if len(kv) > 1}
    return cookies, next_location

def validate_multifactor(base_url, f_sc):
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Cookie': f'JSESSIONID={f_sc["JSESSIONID"]}; glide_user_route={f_sc["glide_user_route"]};',
        'Pragma': 'no-cache',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
    }

    response = requests.get(f'{base_url}/validate_multifactor_auth_code.do', headers=headers)
    next_location = get_next_location(response)
    headers = response.headers
    pairs = headers['Set-Cookie'].split(';')
    cookies = {kv[0].split(',')[-1].strip(): kv[1] for kv in [p.split('=') for p in pairs] if len(kv) > 1}
    output = response.content.decode('utf-8')
    output = output.replace('<', '\n<')
    return cookies, next_location

def validate_code(base_url, f_sc, code=None):
    if not code:
        code = input("Enter 6 digits code:")
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': f'JSESSIONID={f_sc["JSESSIONID"]}; glide_user_route={f_sc["glide_user_route"]};',
        'Origin': f'{base_url}',
        'Referer': f'{base_url}/validate_multifactor_auth_code.do',
        'Sec-Fetch-Dest': 'iframe',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
    }

    data = f'sys_action=sysverb_validate_mfa_code&sys_mfa_check_remembered_browser=false&bfp=&bfp_hash=&sys_web_authentication_successful=false&sys_web_authentication_response=&sys_web_authn_registration_successful=false&sys_web_authn_registration_skipped=false&txtResponse={code}'

    response = requests.post(f'{base_url}/validate_mfa_code.do', headers=headers, data=data)
    next_location = get_next_location(response)
    headers = response.headers
    pairs = headers['Set-Cookie'].split(';')
    cookies = {kv[0].split(',')[-1].strip(): kv[1] for kv in [p.split('=') for p in pairs] if len(kv) > 1}
    return cookies, next_location

def finalize_token(token):
    return f'token_start|{token}|token_end'

def get_snow_token(base_url, username, password, code=None):
    cookies = fetch_server_root(base_url)
    l_cookies, next_location = login(base_url, cookies, username, password)
    cookies.update(l_cookies)
    if next_location == 'validate_multifactor_auth_code.do':
        v_cookies, next_location = validate_multifactor(base_url, cookies)
        cookies.update(v_cookies)
        vc_cookies, next_location = validate_code(base_url, cookies, code)
        cookies.update(vc_cookies)
        if not code and 'glide_user_activity' not in cookies:
            if code:
                return finalize_token('error')
            else:
                print("Wrong code entered")
                return get_snow_token(base_url, username, password)
    elif 'login_redirect.do' in next_location:
        v_cookies, next_location = validate_multifactor(base_url, cookies)
        cookies.update(v_cookies)
    else:
        print("Authentication Error")
        exit(1)
    return finalize_token(cookies["glide_user_activity"])
