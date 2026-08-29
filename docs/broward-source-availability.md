# Broward source availability and official-custodian matrix

Date: 2026-08-28  
County: Broward County, Florida (`12011`)

This closes the source-documentation acceptance gate. “Unavailable” below
means no anonymous record-level endpoint was certified from official pages; it
does not mean the public record does not exist. In those cases, the official
municipal custodian or public-record route is recorded instead.

## Countywide data categories

| Category                 | Official source                                                                                                                                                                            | Availability                                                                                                                                                                                   |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Appraisal                | [Broward County Property Appraiser](https://web.bcpa.net/BcpaClient/#/Record-Search) and its `search.aspx/getParcelInformation` API                                                        | Public JSON; pilot certified 50/50                                                                                                                                                             |
| Parcel geometry          | [BCPA ArcGIS Parcels layer](https://gisweb-adapters.bcpa.net/arcgis/rest/services/BCPA_EXTERNAL_JAN26/MapServer/16)                                                                        | Public ArcGIS JSON; official layer type is `esriGeometryPolygon`; 556,178 features and 534,309 unique folios                                                                                   |
| Building permits         | Municipal sources below; [Broward Building Official Contacts](https://www.broward.org/CodeAppeals/Pages/BuildingContacts.aspx) identifies the official custodians                          | Fragmented across 31 municipalities plus the unincorporated Broward Municipal Services District                                                                                                |
| Florida companies        | [Florida Department of State Division of Corporations](https://dos.fl.gov/sunbiz/) and [quarterly corporate data](https://dos.fl.gov/sunbiz/other-services/data-downloads/quarterly-data/) | Official statewide bulk files; reuse the existing Sunbiz fixed-width loader with Broward ZIP scope                                                                                             |
| Business reputation      | [BBB API](https://developer.bbb.org/) and [API terms](https://developer.bbb.org/terms-of-use)                                                                                              | BBB is not a government registry. API access requires approval; complaint/review data is internal-use-only. No public Broward bulk source is available, and site aggregation is not authorized |
| Tax collector (deferred) | [Broward County Tax Collector](https://browardtax.org/)                                                                                                                                    | Official source identified; not part of the accepted appraisal/permit/Sunbiz/BBB ingest scope                                                                                                  |
| Recorder (deferred)      | [Broward Official Records](https://officialrecords.broward.org/) and [completed index files](https://www.broward.org/RecordsTaxesTreasury/Records/Pages/IndexFiles-Completed.aspx)         | Official source identified; interactive search is Cloudflare-protected. Bulk availability is 10 continuous days of quality-assured images and index data, outside this ingest scope            |

## Permit jurisdictions

Common official evidence:

- [Broward Building Code](https://www.broward.org/building) states that county
  Building Code Services handles the Broward Municipal Services District and
  contracted cities.
- [Broward BCS permit search](https://dpepp.broward.org/BCS/Default.aspx?PossePresentation=ParcelSearchByAddress)
  exposes address/parcel search and lists every Broward municipality. The city
  dropdown is not a countywide custody claim; BCS records are bounded to
  BMSD/unincorporated and records BCS holds for contracted services.
- The local-only
  [BCS POSSE adapter pilot](./broward-bcs-permit-pilot.md) documents exact
  Parcel ID submission, explicit empty/no-match behavior, source provenance,
  detail normalization, and hard request limits.
- The local-only
  [municipal vendor-family prototypes](./broward-municipal-permit-prototypes.md)
  document reusable parsing/checkpoint contracts, bounded official-site
  observations, and executable login/CAPTCHA/records-request skips for the
  remaining Click2Gov, eSuite, Gov-Easy/GeoCivix, SmartGov, OpenGov,
  CommunityCore, MGO Connect, eGovPLUS, and Sunrise routes.
- [Broward ePermits OneStop](https://www.broward.org/ePermits/Pages/Contact.aspx)
  documents the municipal/county split and links the municipal building
  officials/support route.
- The official [Building Official Contacts](https://www.broward.org/CodeAppeals/Pages/BuildingContacts.aspx)
  directory is the fallback custodian route where no anonymous municipal
  record endpoint was certified.

| Jurisdiction                                         | Official record source or custodian evidence                                                                                                                                  | Status                                                                                                                   |
| ---------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Broward Municipal Services District / unincorporated | [BCS parcel/address search](https://dpepp.broward.org/BCS/Default.aspx?PossePresentation=ParcelSearchByAddress)                                                               | Public search and bounded local-only property-first adapter certified                                                    |
| Coconut Creek                                        | [City permit-status search](https://www3.coconutcreek.gov/sd/permit/permit_status_01.asp)                                                                                     | Public permit/property/address search documented                                                                         |
| Cooper City                                          | [City Accela Citizen Access](https://aca-prod.accela.com/COOPER/)                                                                                                             | Official city-linked Accela portal identified                                                                            |
| Coral Springs                                        | [City eTRAKiT](https://etrakit.coralsprings.gov/eTRAKiT/Search/permit.aspx)                                                                                                   | Public search identified; reCAPTCHA blocks unattended certification                                                      |
| Dania Beach                                          | [City Tyler eSuite](https://cityofdaniabeachfl.nwerp.tylerapp.com/nwprod/eSuite.Permits/)                                                                                     | Public permit-type/number/service-address search and reusable bounded parser/checkpoint prototype documented             |
| Davie                                                | [Town eSuite Permits](https://esuite.davie-fl.gov/eSuite.Permits/AdvancedSearchPage/AdvancedSearch.aspx)                                                                      | Bounded search/detail prototype certified; new 2026 submissions use a separate login-gated OAS system                    |
| Deerfield Beach                                      | [Legacy Gov-Easy search](https://apps.gov-easy.com/Home/PermitInspection/Search?clientId=dce877e0-e162-4827-a60d-7249ec4e8fe2)                                                | Legacy search requires numeric CAPTCHA; current GeoCivix is login-gated, so both are explicit skips and records request is required for completeness |
| Fort Lauderdale                                      | [LauderBuild](https://aca3.accela.com/FTL/)                                                                                                                                   | Official Accela basic record search; no login required for basic search                                                  |
| Hallandale Beach                                     | [City Building Division FAQ](https://cohb.org/Faq.aspx?QID=75)                                                                                                                | Official page documents anonymous EnerGov global permit/parcel/address search                                            |
| Hillsboro Beach                                      | [Town CommunityCore portal](https://app.communitycore.com/app/public-portal/c98c7b46-2cba-4ba2-bbd5-7a76966f42dd)                                                             | Account required for status, review comments, fees, and inspections                                                      |
| Hollywood                                            | [City permit-status search](https://apps.hollywoodfl.org/building/PermitStatus.aspx)                                                                                          | Public 1988-present address search plus current Accela; older records use City archives                                  |
| Lauderdale-by-the-Sea                                | [Town Citizenserve](https://www6.citizenserve.com/Portal/PortalController?Action=showHomePage&ctzPagePrefix=Portal_&installationID=117)                                       | Official CAP Government portal exposes permit search, review, fee, and inspection status                                 |
| Lauderdale Lakes                                     | [City OpenGov search](https://lauderdalelakesfl.portal.opengov.com/search)                                                                                                    | Public landing documented, but rendered permit app currently reports inaccessible; fixture-only cursor prototype skips live GraphQL |
| Lauderhill                                           | [City eGovPLUS search](http://egov.lauderhill-fl.gov/eGovPlus83/permit/perm_status.aspx)                                                                                      | Bounded permit/folio/address and detail parser prototype certified on one exact public record                            |
| Lazy Lake                                            | [BCS parcel/address search](https://dpepp.broward.org/BCS/Default.aspx?PossePresentation=ParcelSearchByAddress)                                                               | BCS city list documents county search route; no separate village endpoint                                                |
| Lighthouse Point                                     | [City SmartGov](https://ci-lighthousepoint-fl.smartgovcommunity.com/ApplicationPublic/ApplicationHome)                                                                        | Anonymous bounded search/empty-result and numbered-page parser certified; positive detail remains a blocker             |
| Margate                                              | [City Click2Gov](https://marg-egov.aspgov.com/Click2GovBP/selectpermit.html)                                                                                                  | Public application/address/parcel/name landing and shared bounded Click2Gov parser documented                            |
| Miramar                                              | [City online permitting](https://www.miramarfl.gov/Departments/Building-Planning-Zoning/Building-Permits-Inspections/Online-Permitting)                                       | Official permit/public-record search route documented                                                                    |
| North Lauderdale                                     | [City EnerGov CSS](https://nlselfservice.nlauderdale.org/Energov_prod/SelfService#/home)                                                                                      | Login required; use the official city public-record request for property-wide records                                    |
| Oakland Park                                         | [City Permit Access](https://oaklandparkfl.gov/312/Permit-Access)                                                                                                             | Official page documents legacy pre-2019 search and Tyler post-2019 search                                                |
| Parkland                                             | [MGO Connect](https://www.mgoconnect.org/cp/portal)                                                                                                                           | Free account required for permit-project and inspection-result searches; explicit no-network skip                       |
| Pembroke Park                                        | [Town online permitting](https://www.tppfl.gov/194/Online-Permitting-System)                                                                                                  | Official Gov-Easy status route requires numeric CAPTCHA; explicit skip, with staff email required for submissions        |
| Pembroke Pines                                       | [City Tyler Civic Access](https://pembrokepinesfl-energovweb.tylerhost.net/apps/selfservice)                                                                                  | Official Tyler portal identified                                                                                         |
| Plantation                                           | [City Accela Citizen Access](https://aca.plantation.org/CitizenAccess/Cap/CapHome.aspx?TabName=Building&module=Building)                                                      | Official parcel/address/record search identified                                                                         |
| Pompano Beach                                        | [City Click2Gov](https://c2g.pompanobeachfl.gov/Click2GovBP/selectpermit.html)                                                                                                | Bounded Click2Gov result/detail prototype certified; broad rows cap before detail traversal                              |
| Sea Ranch Lakes                                      | [Broward building-official directory](https://www.broward.org/CodeAppeals/Pages/BuildingContacts.aspx)                                                                        | Official village custodian documented; no anonymous record endpoint certified                                            |
| Southwest Ranches                                    | [Town Citizenserve](https://www6.citizenserve.com/Portal/PortalController?Action=showSearchPage&ctzPagePrefix=Portal_&installationID=117&original_contactID=0&original_iid=0) | CAP Government public permit/address/parcel search documented                                                            |
| Sunrise                                              | [City Building Records](https://www.sunrisefl.gov/departments-services/community-development/building/building-records)                                                       | Microfilm/records request only; official form and custodian fallback configured as no-submit skips, and page still returns 403 here |
| Tamarac                                              | [City Property Permit History](https://tamarac.gov/672/Permit-History)                                                                                                        | Official Click2Gov address/parcel/application/name search and shared bounded parser documented                           |
| West Park                                            | [City Citizenserve](https://www6.citizenserve.com/Portal/PortalController?Action=showSearchPage&ctzPagePrefix=Portal_&installationID=261&original_contactID=0&original_iid=0) | CAP Government public permit/address/parcel search documented                                                            |
| Weston                                               | [City Accela Citizen Access](https://aca-prod.accela.com/weston/Cap/CapHome.aspx?TabName=Building&module=Building)                                                            | Public address/parcel/record/contractor search; city records cover post-1997 history                                     |
| Wilton Manors                                        | [City permit/public-record search guide](https://www.wiltonmanors.gov/DocumentCenter/View/9768/How-to-do-an-online-permit-record-search)                                      | Official Citizenserve parcel/address/permit search; city records portal is the fallback when files cannot be viewed      |

## Operational conclusion

Every requested category now has either a first-party source or a documented
official custodian/unavailability route. This matrix does **not** claim that
all 32 permit jurisdictions have ingestion adapters. Source discovery is
closed; BCS has a local-only property-first adapter, while the remaining
families now have bounded parser/checkpoint prototypes or explicit access
skips. Production municipal coverage still requires tenant-specific transport
certification and positive examples for the blockers documented above.
