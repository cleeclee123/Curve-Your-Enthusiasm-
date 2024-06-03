# Stolen from https://github.com/itsjafer/schwab-api/blob/main/schwab_api/authentication.py

import requests
import pyotp
import re

import asyncio
from playwright.async_api import async_playwright, TimeoutError
from playwright_stealth import stealth_async
from requests.cookies import cookiejar_from_dict


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:{version}) Gecko/20100101 Firefox/"
)
VIEWPORT = {"width": 1920, "height": 1080}


class SessionManager:
    def __init__(self) -> None:
        self.headers = None
        self.session = requests.Session()
        self.playwright = None
        self.browser = None
        self.page = None
        self.go_to_page = None

    # def check_auth(self):
    #     r = self.session.get(urls.account_info_v2())
    #     if r.status_code != 200:
    #         return False
    #     return True

    def get_session(self):
        return self.session

    def login(self, username, password, totp_secret=None):
        result = asyncio.run(self._async_login(username, password, totp_secret))
        return result

    async def _async_login(self, username, password, totp_secret=None):
        self.playwright = await async_playwright().start()
        if self.browserType == "firefox":
            self.browser = await self.playwright.firefox.launch(headless=self.headless)
        else:
            raise ValueError("Only supported browserType is 'firefox'")

        user_agent = USER_AGENT + self.browser.version
        self.page = await self.browser.new_page(
            user_agent=user_agent, viewport=VIEWPORT
        )
        await stealth_async(self.page)

        await self.page.goto(self.go_to_page or "https://www.schwab.com/")

        await self.page.route(
            re.compile(r".*balancespositions*"), self._asyncCaptureAuthToken
        )

        login_frame = "schwablmslogin"
        await self.page.wait_for_selector("#" + login_frame)

        await self.page.frame(name=login_frame).select_option(
            "select#landingPageOptions", index=3
        )

        await self.page.frame(name=login_frame).click('[placeholder="Login ID"]')
        await self.page.frame(name=login_frame).fill(
            '[placeholder="Login ID"]', username
        )

        if totp_secret is not None:
            totp = pyotp.TOTP(totp_secret)
            password += str(totp.now())

        await self.page.frame(name=login_frame).press('[placeholder="Login ID"]', "Tab")
        await self.page.frame(name=login_frame).fill(
            '[placeholder="Password"]', password
        )

        try:
            await self.page.frame(name=login_frame).press(
                '[placeholder="Password"]', "Enter"
            )
            await self.page.wait_for_url(
                re.compile(r"app/trade"), wait_until="domcontentloaded"
            )  # Making it more robust than specifying an exact url which may change.
        except TimeoutError:
            raise Exception(
                "Login was not successful; please check username and password"
            )

        await self.page.wait_for_selector("#_txtSymbol")

        await self._async_save_and_close_session()
        return True

    async def _async_save_and_close_session(self):
        cookies = {
            cookie["name"]: cookie["value"]
            for cookie in await self.page.context.cookies()
        }
        self.session.cookies = cookiejar_from_dict(cookies)
        await self.page.close()
        await self.browser.close()
        await self.playwright.stop()

    async def _asyncCaptureAuthToken(self, route):
        self.headers = await route.request.all_headers()
        await route.continue_()
