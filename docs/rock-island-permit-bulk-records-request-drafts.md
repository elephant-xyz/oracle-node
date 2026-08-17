# Rock Island County permit bulk-export/public-records request drafts

**Prepared:** 2026-08-13
**Status:** Drafts only — not submitted
**Scope:** Moline, City of Rock Island, East Moline, and Carbon Cliff permit history

These drafts request existing electronic records and exports. They do not authorize a
custom paid report, agree to fees, represent the requester's commercial-purpose status,
or grant permission to send anything. The operator must complete the bracketed fields and
verify each destination immediately before submission.

## Common request boundaries

For each jurisdiction, request all available electronic permit history from current and
legacy systems in an existing machine-readable CSV, JSON, relational-table export,
database extract, or comparable structured format. If no single export exists, ask for
the least burdensome combination of existing exports.

Requested fields, where maintained and public:

- permit/record number and stable source-system record identifier;
- issuing jurisdiction and department;
- permit type, class/work class/category, and status;
- application, issue, expiration, finalization/closure, and other material status dates;
- work description and project/work category;
- declared project valuation or estimated job value;
- parcel/PIN/APN and public site/work address;
- contractor **business** name, registration/license type, and license number;
- inspection identifier, type, status, requested/scheduled/completed/result dates, and
  result;
- parent/related permit identifiers needed to preserve record relationships; and
- source-system name, tenant/jurisdiction code, source record ID, and export timestamp.

Explicit exclusions:

- personal phone numbers or personal email addresses;
- payment-card, bank-account, authentication, password, credential, or security data;
- private portal messages, uploaded identity documents, or non-public attachments;
- applicant/homeowner/occupant person-level fields not necessary to identify the public
  permit record;
- contractor personal contact fields when contractor business identity and public
  license fields are sufficient; and
- any field the public body determines is exempt or requires redaction.

Each request also asks for:

- total record count and counts by year, permit type, and issuing jurisdiction if readily
  available;
- earliest and latest record dates and any known historical gaps;
- data dictionary/schema, code lists, relationship keys, and null/sentinel conventions;
- current refresh/export cadence and whether periodic public exports are available;
- source-system and legacy-system boundaries, including migration dates;
- estimated export fees or staff charges **before** work begins;
- a lower-cost existing-export option if the initial scope would incur fees; and
- applicable use, attribution, licensing, confidentiality, or redistribution terms,
  including whether a privacy-reviewed non-PII derivative may be republished publicly.

## Draft 1 — City of Moline

### Verified destination details

- Official FOIA page:
  `https://www.moline.il.us/408/Freedom-of-Information-Request`
- Submission route: use the current **Non-Police (all other city business) FOIA Request
  Form** linked from that official page.
- Recipient role: Deputy City Clerk / City Clerk's Office.
- Mailing address: Moline City Hall, 619 16th Street, Moline, IL 61265.
- Phone: 309-524-2003.
- The current official FOIA page says email submissions are no longer accepted.
- Technical permit contact published by the Building/Inspections Division:
  `buildingpermits@moline.il.us`. This is **not verified as a FOIA submission address**;
  use it only if the City Clerk redirects technical questions there.
- Permit portal:
  `https://moli.csqrcloud.com/community-etrakit/`

### Unverified before sending

- The exact current direct URL behind the non-police online form should be opened from
  the official FOIA page on the sending date.
- The records custodian for CentralSquare/eTRAKiT exports is not identified.
- Available pre-eTRAKiT history and whether it was migrated are unknown.

### Copy-ready draft

**Subject:** Request for machine-readable Moline permit history and export metadata

To the Deputy City Clerk / FOIA Officer:

Under the Illinois Freedom of Information Act, I request an existing electronic export
of all available City of Moline permit history, including records in the current
CentralSquare/eTRAKiT system and any retained legacy permit system.

Please provide the records in an existing machine-readable CSV, JSON, database extract,
or comparable structured format rather than PDF where reasonably available. Requested
public fields are: record number and system ID; permit type/work class/category and
status; application, issue, expiration, and final/closure dates; work
description/category; public project valuation; parcel/PIN/APN; site/work address;
contractor business name and public license type/number; inspection IDs, types,
statuses, dates, and results; related-record IDs; and source-system/tenant identifiers.

Please exclude personal phone/email, payment-card or bank data, credentials, private
portal messages, identity documents, and unnecessary applicant/homeowner/occupant
person-level fields. Contractor business/license fields are requested, but personal
contractor contact fields are not.

Please also state:

- total record count, earliest/latest dates, and known gaps;
- whether legacy history was migrated into eTRAKiT and the migration/cutover date;
- counts by year and permit type if readily available;
- the data dictionary/schema, code lists, and relationship keys;
- refresh/export cadence and any existing recurring public export;
- applicable attribution, use, and redistribution terms, including whether a
  privacy-reviewed non-PII derivative may be publicly republished; and
- any estimated fee before work begins, with a lower-cost existing-export option if
  applicable.

Electronic delivery is preferred. This request does not authorize fees. Please provide a
written estimate and await approval before incurring any charge.

Requester: [NAME / ORGANIZATION]
Response email: [EMAIL]
Mailing address/phone if required by the form: [DETAILS]
Commercial-purpose declaration: [OPERATOR MUST ANSWER ACCURATELY]

## Draft 2 — City of Rock Island

### Verified destination details

- Official FOIA page: `https://rigov.org/78/Freedom-of-Information`
- Official City Clerk online form:
  `https://www.rigov.org/FormCenter/Freedom-of-Information-Act-Requests-7/City-Clerk-Freedom-of-Information-Reques-52`
- Recipient: Office of the City Clerk.
- Mailing/personal-delivery address: City Hall, 1528 Third Avenue, Rock Island, IL 61201.
- City Clerk phone: 309-732-2010.
- The official page permits written requests by email, mail, or personal delivery, but a
  current City Clerk FOIA email address was **not verified** in the reviewed source. Use
  the official online form unless the operator verifies an email address.
- Permit portal:
  `https://cityofrockislandil-energovweb.tylerhost.net/apps/selfservice#/home`
- Official monthly issued-permit reports:
  `https://www.rigov.org/1276/Permit-Reports`

### Unverified before sending

- The Tyler EnerGov/Civic Access production cutover and earliest portal date are unknown.
- The monthly report index currently exposes 2017 through April 2026, but it is unknown
  whether earlier records or fuller lifecycle data exist in another system.
- The exact Inspections Division data custodian/export owner is not identified.

### Copy-ready draft

**Subject:** Request for machine-readable City of Rock Island permit history

To the Office of the City Clerk / FOIA Officer:

Under the Illinois Freedom of Information Act, I request an existing electronic export
of all available City of Rock Island permit history, including current Tyler
EnerGov/Civic Access records, records underlying the City's monthly permit reports, and
any retained or unmigrated legacy permit data.

Please provide the records in an existing machine-readable CSV, JSON, database extract,
or comparable structured format rather than PDF where reasonably available. Requested
public fields are: permit number and system ID; type/work class/category and status;
application, issue, expiration, and final/closure dates; work description/category;
public project valuation; parcel/PIN/APN; site/work address; contractor business name and
public license type/number; inspection IDs, types, statuses, dates, and results;
related-record IDs; and source-system/tenant identifiers.

Please exclude personal phone/email, payment-card or bank data, credentials, private
portal messages, identity documents, and unnecessary applicant/homeowner/occupant
person-level fields. Contractor business/license fields are requested, but personal
contractor contact fields are not.

Please also state:

- total record count, earliest/latest dates, and known gaps;
- which records predate the monthly reports and which records were migrated into Tyler;
- Tyler/legacy migration or cutover dates;
- counts by year and permit type if readily available;
- the data dictionary/schema, code lists, and relationship keys;
- refresh/export cadence and any existing recurring public export;
- applicable attribution, use, and redistribution terms, including whether a
  privacy-reviewed non-PII derivative may be publicly republished; and
- any estimated fee before work begins, with a lower-cost existing-export option if
  applicable.

Electronic delivery is preferred. This request does not authorize fees. Please provide a
written estimate and await approval before incurring any charge.

Requester: [NAME / ORGANIZATION]
Response email: [EMAIL]
Mailing address/phone required by the form: [DETAILS]
Commercial-purpose declaration: [OPERATOR MUST ANSWER ACCURATELY]

## Draft 3 — City of East Moline

### Verified destination details

- Official city homepage links the FOIA submission portal:
  `https://cityofeastmolineil.nextrequest.com/`
- Recipient: FOIA Officer, City of East Moline.
- Mailing address: City Hall Annex, 912 16th Avenue, East Moline, IL 61244.
- Phone: 309-752-1599.
- Email published on the official FOIA form: `foia@eastmoline.com`.
- Permit portal:
  `https://eastmolinepermit.portal.iworq.net/portalhome/eastmolinepermit`
- The official permit materials include East Moline and Carbon Cliff application paths.

### Unverified before sending

- Whether NextRequest requires account creation should be checked by the operator; do not
  create an account automatically.
- The exact iWorQ export custodian and available history are unknown.
- Whether every Carbon Cliff record carries a reliable issuing-jurisdiction field is
  unknown.

### Copy-ready draft

**Subject:** Request for machine-readable East Moline and distinguishable Carbon Cliff permit history

To the City of East Moline FOIA Officer:

Under the Illinois Freedom of Information Act, I request an existing electronic export
of all available permit history administered by the City of East Moline, including the
current iWorQ system, retained legacy systems, and permit/inspection records administered
for the Village of Carbon Cliff.

Please provide the records in an existing machine-readable CSV, JSON, database extract,
or comparable structured format rather than PDF where reasonably available. Requested
public fields are: permit number and system ID; **issuing/served jurisdiction**; permit
type/work class/category and status; application, issue, expiration, and final/closure
dates; work description/category; public project valuation; parcel/PIN/APN; site/work
address; contractor business name and public license type/number; inspection IDs, types,
statuses, dates, and results; related-record IDs; and source-system/tenant identifiers.

Please confirm specifically:

1. whether Carbon Cliff records can be distinguished reliably from East Moline records;
2. the field/code that identifies the jurisdiction;
3. whether the distinction applies to permits, contractor registrations, inspections,
   payments, and legacy records; and
4. whether any Carbon Cliff permit classes are maintained outside East Moline/iWorQ.

Please exclude personal phone/email, payment-card or bank data, credentials, private
portal messages, identity documents, and unnecessary applicant/homeowner/occupant
person-level fields. Contractor business/license fields are requested, but personal
contractor contact fields are not.

Please also state:

- total record count, earliest/latest dates, and known gaps, separately for East Moline
  and Carbon Cliff if readily available;
- legacy/iWorQ migration or cutover dates;
- counts by year, permit type, and jurisdiction if readily available;
- the data dictionary/schema, jurisdiction code list, and relationship keys;
- refresh/export cadence and any existing recurring public export;
- applicable attribution, use, and redistribution terms, including whether a
  privacy-reviewed non-PII derivative may be publicly republished; and
- any estimated fee before work begins, with a lower-cost existing-export option if
  applicable.

Electronic delivery is preferred. This request does not authorize fees. Please provide a
written estimate and await approval before incurring any charge.

Requester: [NAME / ORGANIZATION]
Response email: [EMAIL]
Mailing address/phone if required: [DETAILS]
Commercial-purpose declaration: [OPERATOR MUST ANSWER ACCURATELY]

## Draft 4 — Village of Carbon Cliff

### Verified destination details

- Official open-records page: `https://carboncliff.gov/open-records`
- Recipient: Village Clerk / Village of Carbon Cliff.
- Contact listed on the official page: Meagan Stang, Administrative Assistant, Village
  Clerk, and Collector.
- Mailing/hand-delivery address: 1001 Mansur Avenue, Carbon Cliff, IL 61239.
- Phone: 309-792-8235.
- Email: `clerk@carboncliff.com`.
- The official page accepts a written request by electronic mail, fax, letter, hand
  delivery, or mail.
- Official permit/delegation page:
  `https://carboncliff.gov/permits-zoning-and-floodplain`
- Verified delegation: East Moline handles Carbon Cliff plumbing, mechanical, building,
  and electrical permits and inspections after Village Hall clearance.

### Unverified before sending

- Whether Carbon Cliff keeps copies of delegated East Moline/iWorQ records is unknown.
- The system and retention period for Village Hall clearance, zoning, floodplain, and
  pre-delegation permit records are unknown.
- The fax number was not verified and should not be used until confirmed.

### Copy-ready draft

**Subject:** Request for machine-readable Carbon Cliff permit, clearance, and delegated-record history

To the Village Clerk / FOIA Officer:

Under the Illinois Freedom of Information Act, I request existing electronic records for
all available Village of Carbon Cliff permit-related history. This includes:

1. Village Hall pre-clearance, zoning, floodplain, or approval records associated with
   building, plumbing, mechanical, and electrical work;
2. any permit/inspection records retained by the Village from the delegated East Moline
   process;
3. records from any legacy or pre-delegation permit system; and
4. the delegation agreement, written data-sharing procedure, or field/code used to
   identify Carbon Cliff records in East Moline/iWorQ, if public and readily available.

Please provide record data in an existing machine-readable CSV, JSON, database extract,
or comparable structured format rather than PDF where reasonably available. Requested
public fields are: permit/clearance number and system ID; issuing/served jurisdiction;
type/work class/category and status; application, clearance, issue, expiration, and
final/closure dates; work description/category; public project valuation; parcel/PIN/APN;
site/work address; contractor business name and public license type/number; inspection
IDs, types, statuses, dates, and results; related-record IDs; and source-system/tenant
identifiers.

Please exclude personal phone/email, payment-card or bank data, credentials, private
portal messages, identity documents, and unnecessary applicant/homeowner/occupant
person-level fields. Contractor business/license fields are requested, but personal
contractor contact fields are not.

Please also state:

- which responsive records are held by Carbon Cliff versus East Moline;
- whether Carbon Cliff records can be distinguished reliably in East Moline/iWorQ and by
  which field/code;
- total record count, earliest/latest dates, and known gaps;
- counts by year and permit type if readily available;
- delegation, legacy-system, or migration effective dates;
- the data dictionary/schema, jurisdiction code list, and relationship keys;
- refresh/export cadence and any existing recurring public export;
- applicable attribution, use, and redistribution terms, including whether a
  privacy-reviewed non-PII derivative may be publicly republished; and
- any estimated fee before work begins, with a lower-cost existing-export option if
  applicable.

If East Moline is the sole custodian of delegated permit/inspection records, please
identify the appropriate custodian and, if permitted, forward or transfer that portion of
the request.

Electronic delivery is preferred. This request does not authorize fees. Please provide a
written estimate and await approval before incurring any charge.

Requester: [NAME / ORGANIZATION]
Response email: [EMAIL]
Mailing address/phone if required: [DETAILS]
Commercial-purpose declaration: [OPERATOR MUST ANSWER ACCURATELY]

## Operator send-and-track checklist

- [ ] Insert requester name, organization, response email, address, and phone where the
      destination requires them.
- [ ] Determine and disclose commercial-purpose status accurately; do not reuse a default
      answer without operator/legal review.
- [ ] Open each official FOIA/open-records page on the sending date and verify the
      recipient, portal URL, mailing address, and accepted submission method.
- [ ] Submit each draft separately to the correct non-police/general-records destination.
- [ ] Do not upload credentials, source captures, scraped records, or unnecessary
      personal information with the request.
- [ ] Do not agree to fees. If a fee estimate arrives, record it and obtain explicit
      operator approval before authorizing work or payment.
- [ ] Track: jurisdiction, sent date/time, submission channel, request ID, statutory due
      date, assigned responder, clarification requests, extension date, fee estimate,
      status, and final response date.
- [ ] Preserve the original submitted text, confirmation page/email, response messages,
      denial/exemption citations, and attachments in a restricted records-request folder.
- [ ] For every delivered file, record filename, byte count, SHA-256 checksum, format,
      row count, earliest/latest dates, schema/data dictionary, and stated terms.
- [ ] Compare East Moline and Carbon Cliff responses for a stable jurisdiction field
      before combining records.
- [ ] Route any license, attribution, commercial-use, or redistribution restriction for
      legal/operator review before ingestion or publication.
- [ ] Keep owner/applicant/person-level fields private by default. No IPFS/public export
      may proceed until the final field allow-list proves excluded fields are absent.
