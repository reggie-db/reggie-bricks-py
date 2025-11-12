import functools
import pathlib
import pprint
import random
import re
from dataclasses import dataclass, field
from datetime import date, timedelta

import markovify
import sh
from bs4 import BeautifulSoup
from faker import Faker
from jinja2 import Environment, FileSystemLoader, select_autoescape
from random_address import real_random_address
from reggie_core import funcs


def generate(info: "FranchiseAgreementInfo" = None) -> str:
    if info is None:
        info = FranchiseAgreementInfo()
    tpl = _template_env().get_template("franchise_agreement.html")

    sections = [
        "cover",
        "recitals",
        "grant",
        "term_and_renewal",
        "fees",
        "operating_standards",
        "training_qc",
        "insurance_indemnity",
        "confidentiality",
        "termination",
        "post_termination",
        "dispute_resolution",
        "miscellaneous",
        "signatures",
    ]

    random.shuffle(sections)

    generated_by_secno = {}

    for i in range(len(sections)):
        clauses = []
        for j in range(random.randint(1, 2)):
            for k in range(random.randint(1, 3)):
                letter = chr(ord("A") + k)
                clauses.append(_numbered_clause(i + 1, f"{j + 1}.{letter}", 8))

        generated_by_secno[i + 1] = clauses

    html = tpl.render(
        **funcs.dump(info, recursive=False),
        sections_order=sections,
        generated_by_secno=generated_by_secno,
    )
    return html


@functools.cache
def _faker() -> Faker:
    return Faker("en_US")


def _html_to_text(path: pathlib.Path) -> str:
    html = path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "html.parser")

    # optional: remove nav, scripts, styles
    for tag in soup(["script", "style", "nav", "header", "footer"]):
        tag.decompose()

    # keep headings and paragraphs only
    parts = []
    for el in soup.find_all(["h1", "h2", "h3", "h4", "p", "li"]):
        txt = el.get_text(separator=" ", strip=True)
        txt = re.sub(r"\s+", " ", txt)
        if txt:
            parts.append(txt)

    # join with newlines so paragraphs are preserved
    return "\n".join(parts)


def _strip_section_numbers(text: str) -> str:
    """
    Remove legal-style section numbers such as:
    9., 9.1., 9.A., 11.3.B., 11.3.B.2, etc.
    Allows optional spaces after dots and ignores case.
    """
    pattern = r"""
        (?:                # non-capturing group for full section pattern
            \b             # start at a word boundary
            \d+            # one or more digits (e.g., 9)
            (?:            # zero or more groups of:
                \s*\.\s*   # dot with optional spaces
                [A-Za-z0-9]+  # digits or letters (A, 1, B, etc.)
            )*             # can repeat (e.g., 9.1.A.2)
            \s*\.\s*       # optional trailing dot
        )
    """
    return re.sub(pattern, "", text, flags=re.VERBOSE | re.IGNORECASE)


def _replace_brand(text: str, brand: str, replacement: str = "COMPANY") -> str:
    """
    Replace a brand name in text ignoring case, arbitrary spacing, and hyphens,
    but without matching trailing spaces beyond the brand itself.
    """
    # Normalize brand: treat hyphens as spaces
    brand_clean = re.sub(r"[-\s]+", " ", brand.strip())

    # Build pattern: allow any number of spaces or hyphens between characters
    chars = list(brand_clean)
    parts = [rf"{re.escape(c)}[ \t\-]*" for c in chars[:-1] if c.strip()]
    parts.append(re.escape(chars[-1]))  # last char, no trailing match
    pattern = "".join(parts)

    # Add word boundaries
    pattern = rf"\b{pattern}\b"

    return re.sub(pattern, replacement, text, flags=re.IGNORECASE)


@functools.cache
def _legal_corpus() -> str:
    # build a corpus from many HTML files
    path = pathlib.Path(__file__).parent / "samples"
    corpus = ""
    for p in path.glob("*.html"):
        text = _html_to_text(p)
        text = _strip_section_numbers(text).strip()
        corpus += text + "\n"

    for brand in [
        "ROCKY MOUNTAIN CHOCOLATE FACTORY",
        "BURGER KING",
        "WENDY'S",
        "BUFFALO WILD WINGS",
        "Dunkin Donuts",
        "Baskin Robbins",
    ]:
        corpus = _replace_brand(corpus, brand)
    return corpus


@functools.cache
def _text_model() -> markovify.Text:
    return markovify.Text(_legal_corpus(), state_size=2)


@functools.cache
def _para_model() -> markovify.NewlineText:
    return markovify.NewlineText(_legal_corpus(), state_size=2)


def _legal_sentence(tries=100):
    return _text_model().make_sentence(tries=tries)


def _legal_paragraph(sentences=6):
    return " ".join(filter(None, [_legal_sentence() for _ in range(sentences)]))


def _numbered_clause(sec, sub, sentences=8):
    return f"{sec}.{sub}.    " + _legal_paragraph(sentences)


@functools.cache
def _template_env() -> Environment:
    return Environment(
        loader=FileSystemLoader(pathlib.Path(__file__).parent / "templates"),
        autoescape=select_autoescape(["html", "xml"]),
    )


def _fake_phone_number() -> str:
    phone_number = _faker().phone_number()
    phone_number = phone_number.replace(".", "-")
    phone_number = phone_number.replace("x", " x")
    phone_number = re.sub(r"(?<=\))(?=\d)", ") ", phone_number)
    return phone_number


@dataclass
class Address:
    street: str
    city: str
    state: str
    zip_code: str
    latitude: float
    longitude: float

    @staticmethod
    def fake() -> "Address":
        # random-address gives realistic US pairings
        data = real_random_address()
        street = data.get("address1")
        city = data.get("city")
        state = data.get("state")
        zip_code = data.get("postalCode")
        latitude = float(data.get("latitude", random.uniform(-90, 90)))
        longitude = float(data.get("longitude", random.uniform(-180, 180)))

        return Address(street, city, state, zip_code, latitude, longitude)

    def __str__(self) -> str:
        return f"{self.street}, {self.city}, {self.state} {self.zip_code}"


@dataclass
class FranchiseAgreementInfo:
    brand_name: str = field(default_factory=lambda: _faker().company())
    franchisor_name: str | None = field(default=None)
    franchisee_company: str = field(default_factory=_faker().company)
    authorized_signer: str = field(default_factory=_faker().name)
    franchisor_authorized_signer: str = field(default_factory=_faker().name)
    execution_date: date = field(
        default_factory=lambda: _faker().date_between_dates(
            date_start=date(2024, 1, 1), date_end=date(2025, 12, 31)
        )
    )
    address: Address = field(default_factory=Address.fake)
    email: str = field(default_factory=_faker().email)
    phone: str = field(default_factory=_fake_phone_number)
    currency: str = "USD"
    initial_fee: int = field(default_factory=lambda: random.randint(25000, 75000))
    royalty_pct: int = field(default_factory=lambda: random.randint(3, 7))
    ad_fund_pct: int = field(default_factory=lambda: random.randint(2, 5))
    term_years: int = field(default_factory=lambda: random.choice([10, 15, 20]))

    def __post_init__(self):
        if not self.franchisor_name:
            self.franchisor_name = self.brand_name + " Corporation"

    @property
    def governing_state(self) -> str:
        return self.address.state

    @property
    def venue_county(self) -> str:
        return f"{self.address.city} County"

    @property
    def franchise_execution_date(self) -> date:
        return self.execution_date - timedelta(days=random.randint(1, 7))


if __name__ == "__main__":
    info = FranchiseAgreementInfo(
        brand_name="Baskin Robbins",
    )
    pprint.pprint(funcs.dump(info))
    html = generate_franchise_agreement(info)
    with open(
        pathlib.Path(__file__).parent / "dev-local" / "agreement_with_generated.html",
        "w",
        encoding="utf-8",
    ) as f:
        f.write(html)
        path = pathlib.Path(f.name).absolute()
    sh.open(path)
