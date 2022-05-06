const moment = require("moment");
const { query, where, getDoc, getDocs, collection, collectionGroup, setDoc, doc, limit, orderBy, deleteDoc } = require("firebase/firestore");
const { db } = require("./database");
const { Facebook } = require("roasfacebook");
const { logroupby, lokeyby, pipeLog, lofilter, louniqby, lomap, isEmail, isIPv4, isIPv6, lochunk, toSearchParams, loorderby } = require("helpers");
const {
    pick,
    map,
    path,
    pipe,
    reject,
    sum,
    flatten,
    values,
    mergeDeepRight,
    head,
    prop,
    objOf,
    identity,
    of,
    dissoc,
    applySpec,
    uniq,
    filter,
    keys,
    defaultTo,
    curry,
    sortBy,
    last,
    paths,
    flip,
    not,
    anyPass,
    split,
    hasPath,
    omit,
} = require("ramda");
const { size, isUndefined, toNumber, isNaN, compact, isEmpty, flattenDeep, toLower, toUpper, toString } = require("lodash");
const {
    from,
    of: rxof,
    zip,
    map: rxmap,
    concatMap,
    catchError,
    throwError,
    lastValueFrom,
    defaultIfEmpty,
    reduce: rxreduce,
    tap,
    mapTo,
    filter: rxfilter,
    withLatestFrom,
} = require("rxjs");
const { get, all, mod, matching } = require("shades");
const pluralize = require("pluralize");

const toFixed = (fixed, num) => {
    var re = new RegExp("^-?\\d+(?:.\\d{0," + (fixed || -1) + "})?");
    return num.toString().match(re)[0];
};

const raToFixed = flip(toFixed);

const numOrZero = (value) => {
    return isNaN(value) || !isFinite(value) || isUndefined(value) ? 0 : toNumber(value);
};

const numOrDefault = (value, defaultValue) => {
    return numOrZero(value) == 0 ? defaultValue : numOrZero(value);
};

let flipNumOrDefault = flip(numOrDefault);

let get_id_type = (id) => {
    let is_ipv4 = isIPv4(id);
    let is_ipv6 = isIPv6(id);
    let is_email = isEmail(id);

    if (is_ipv4) {
        return { type: "ipv4", id };
    }

    if (is_ipv6) {
        return { type: "ipv6", id };
    }

    if (is_email) {
        return { type: "email", id };
    }

    return { type: "name", id };
};

const get_customer_ids_type = (ids) => {
    return compact(map((id) => get_id_type(id), ids));
};

const ClickFunnels = {};

ClickFunnels.user_db_docs = ({ emails, user_id }) => {
    let func_name = "ClickFunnels:user_db_docs";
    console.log(func_name);

    let email_query = query(collection(db, "clickfunnels"), where("email", "in", emails), where("user_id", "==", user_id));

    let user_docs = from(getDocs(email_query))
        .pipe(rxmap((data) => data.docs.map((doc) => doc.data())))
        .pipe(
            rxmap(
                map(
                    applySpec({
                        email: path(["email"]),
                        contact_email: path(["contact_profile", "email"]),
                        ip: path(["ip"]),
                    })
                )
            ),
            rxmap(pipe(map(paths([["email"], ["contact_email"], ["ip"]])), flatten, uniq)),
            concatMap((user_ids) => {
                if (!isEmpty(user_ids)) {
                    let ids_chunked = chunk(user_ids, 10);

                    return from(ids_chunked).pipe(
                        concatMap((chunk_data) => {
                            let db_query = query(collection(db, "users"), where("ids", "array-contains-any", chunk_data));
                            return from(getDocs(db_query)).pipe(rxmap((data) => data.docs.map((doc) => ({ ...doc.data(), user_id: doc.id }))));
                        }),
                        rxreduce((prev, curr) => [...prev, ...curr])
                    );
                } else {
                    return rxof([]);
                }
            })
        );

    return user_docs;
};

ClickFunnels.ip_docs_with_ad_data = ({ ip_ids, user_id }) => {
    let func_name = "ClickFunnels:ip_docs_with_ad_data";
    console.log(func_name);

    if (!ip_ids) return throwError(`error:${func_name}:no ip_ids`);
    if (!user_id) return throwError(`error:${func_name}:no user_id`);

    let shopping_cart_ip_docs = rxof(ip_ids).pipe(
        rxfilter((value) => !isUndefined(value) && !isEmpty(value)),
        concatMap(() => {
            let clickfunnel_ip_query = query(collection(db, "clickfunnels"), where("ip", "in", ip_ids), where("user_id", "==", user_id));
            return from(getDocs(clickfunnel_ip_query)).pipe(rxfilter((value) => !isUndefined(value)));
        }),
        rxmap((data) => data.docs.map((doc) => doc.data()))
    );

    return shopping_cart_ip_docs;
};

ClickFunnels.email_docs_with_ad_data = ({ email_ids, user_id }) => {
    let func_name = "ClickFunnels:email_docs_with_ad_data";
    console.log(func_name);

    if (!email_ids) return throwError(`error:${func_name}:no email_ids`);
    if (!user_id) return throwError(`error:${func_name}:no user_id`);

    let shopping_cart_email_docs = rxof(email_ids).pipe(
        rxfilter((value) => !isUndefined(value) && !isEmpty(value)),
        concatMap(() => {
            let clickfunnel_emails_query = query(collection(db, "clickfunnels"), where("email", "in", email_ids), where("user_id", "==", user_id));
            return from(getDocs(clickfunnel_emails_query)).pipe(rxfilter((value) => !isUndefined(value)));
        }),
        rxmap((data) => data.docs.map((doc) => doc.data()))
    );

    return shopping_cart_email_docs;
};

const Event = {};
const Events = {};

Event.params = (event) => {
    let { search, href, referrer } = event;
    let params = {};

    let [, referrer_params] = pipe(defaultTo(""), split("?"))(referrer);
    if (referrer_params) {
        params = { ...toSearchParams(`?${referrer_params}`), ...params };
    }

    let [, href_params] = pipe(defaultTo(""), split("?"))(href);
    if (href_params) {
        params = { ...toSearchParams(`?${href_params}`), ...params };
    }

    if (href_params) {
        params = { ...toSearchParams(search), ...params };
    }

    // console.log("Event.params");
    // console.log(params);

    return params;
};

Event.fb_ad_data = async ({ ad_id, date, user_id, fb_ad_account_id } = {}) => {
    let func_name = `Event.fb_ad_data`;
    console.log(func_name);
    console.log(date);

    if (!ad_id) return throwError(`error:${func_name}:no ad_id`);
    if (!date) return throwError(`error:${func_name}:no date`);
    if (!user_id) return throwError(`error:${func_name}:no user_id`);
    if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

    let facebook = Facebook({ user_id });

    let ad_from_db_response = from(Facebook({ user_id }).ad.get_from_db({ ad_id })).pipe(concatMap(identity));

    let ad_from_db = await lastValueFrom(ad_from_db_response);

    const get_ad_details = (ad) => {
        if (ad.id) {
            let { adset_id, campaign_id } = ad;
            let adset = from(facebook.adset.get({ adset_id, date, fb_ad_account_id })).pipe(rxmap(pipe(values, head)));
            let campaign = from(facebook.campaign.get({ campaign_id, date, fb_ad_account_id })).pipe(rxmap(pipe(values, head)));

            return zip([adset, campaign]).pipe(
                rxmap(([adset, campaign]) => {
                    let { name: campaign_name } = campaign;
                    let { name: adset_name } = adset;
                    return {
                        ...ad,
                        campaign_name,
                        adset_name,
                        asset_id: ad.id,
                        asset_name: ad.name,
                        details: {
                            asset_id: ad.id,
                            asset_name: ad.name,
                            campaign_id: ad.campaign_id,
                            campaign_name,
                            adset_id: ad.adset_id,
                            adset_name,
                            ad_id: ad.id,
                            ad_name: ad.name,
                        },
                    };
                })
            );
        } else {
            return rxof({ ...ad });
        }
    };

    if (isEmpty(ad_from_db)) {
        console.log("no_from_dbbbbbbbb");

        let fb_response = await lastValueFrom(from(facebook.ad.get({ ad_id, date, fb_ad_account_id })));

        return rxof(fb_response)
            .pipe(rxmap(pipe(values, head)))
            .pipe(
                concatMap(get_ad_details),
                rxmap((asset) => ({ ...asset, ad_name: asset.name })),
                tap((asset) => Facebook({ user_id }).ad.set({ ...asset, date })),
                rxmap(pick(["account_id", "campaign_id", "adset_id", "ad_id", "campaign_name", "adset_name", "name"])),
                rxmap(pipeLog),
                rxmap((asset) => ({
                    account_id: asset.account_id,
                    campaign_id: asset.details.campaign_id,
                    campaign_name: asset.details.campaign_name,
                    adset_id: asset.details.adset_id,
                    adset_name: asset.details.adset_name,
                    ad_id: asset.details.ad_id,
                    ad_name: asset.details.ad_name,
                    name: asset.details.ad_name,
                })),
                catchError((error) => {
                    console.log("errrrrrrrorno_from_dbbbbbbbb");
                    console.log({ ad_id, error });
                    return rxof({ ad_id, error });
                })
            );
    } else {
        console.log("fromdbbbbbb");
        return rxof(ad_from_db).pipe(
            concatMap((asset) =>
                asset.details
                    ? rxof(asset)
                    : from(get_ad_details(asset)).pipe(
                          rxmap((asset) => ({ ...asset, ad_name: asset.name })),
                          tap((asset) => Facebook({ user_id }).ad.set({ ...asset, date }))
                      )
            ),
            rxmap(pipeLog),
            rxmap((asset) => ({
                account_id: asset.account_id,
                campaign_id: asset.details.campaign_id,
                campaign_name: asset.details.campaign_name,
                adset_id: asset.details.adset_id,
                adset_name: asset.details.adset_name,
                ad_id: asset.details.ad_id,
                ad_name: asset.details.ad_name,
                name: asset.details.ad_name,
            })),
            catchError((error) => {
                console.log("errrrrrrrorfromdbbbbbb");
                console.log({ ad_id, error });
                return rxof({ ad_id, error });
            })
        );
    }
};

Event.get_ad_id = ({ fb_ad_id, h_ad_id, ad_id }) => {
    // console.log("get_ad_id");
    // console.log(obj);

    if (ad_id) {
        return ad_id;
    }

    if (fb_ad_id && h_ad_id) {
        if (fb_ad_id == h_ad_id) {
            return fb_ad_id;
        }

        if (fb_ad_id !== h_ad_id) {
            return h_ad_id;
        }
    }

    if (fb_ad_id && !h_ad_id) {
        return fb_ad_id;
    }

    if (h_ad_id && !fb_ad_id) {
        return h_ad_id;
    }
};

Event.get_utc_timestamp = (value) => {
    console.log("get_utc_timestamp");

    let timestamp;

    if (get("created_at_unix_timestamp")(value)) {
        timestamp = get("created_at_unix_timestamp")(value);
        console.log(timestamp);
        return timestamp;
    }

    if (get("utc_unix_time")(value)) {
        let timestamp = get("utc_unix_time")(value);
        console.log(timestamp);
        return timestamp;
    }

    if (get("utc_iso_datetime")(value)) {
        let timestamp = pipe(get("utc_unix_time"), (value) => moment(value).unix())(value);
        console.log(timestamp);
        return timestamp;
    }

    timestamp = get("unix_datetime")(value);
    console.log(timestamp);

    if (!timestamp) {
        console.log("notimestamp");
        console.log(value);
    }

    return timestamp;
};

const Timestamp = {};

Timestamp.toUTCDigit = (timestamp) => {
    let regex_expression = /^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$/;

    let date = moment(timestamp, "X").format("YYYY-MM-DD");
    let date_is_valid = regex_expression.test(date);

    if (!date_is_valid) {
        return timestamp / 1000;
    } else {
        return timestamp;
    }
};

Events.ad_data = ({ events, event_from_type, email, fb_ad_account_id, user_id, date }) => {
    let func_name = `Event.ad_data`;
    console.log(func_name);

    if (!events) return throwError(`error:${func_name}:no events`);
    if (!event_from_type) return throwError(`error:${func_name}:no from`);
    if (!email) return throwError(`error:${func_name}:no email`);
    if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);
    if (!user_id) return throwError(`error:${func_name}:no user_id`);
    if (!date) return throwError(`error:${func_name}:no date`);

    return rxof(events).pipe(
        rxmap(
            pipe(
                map((event) => ({
                    ad_id: Event.get_ad_id(event),
                    timestamp: Math.trunc(Timestamp.toUTCDigit(Math.trunc(Event.get_utc_timestamp(event)))),
                })),
                map((event) => ({ ...event, date: moment(event.timestamp, "X").format("YYYY-MM-DD") })),
                reject((event) => !event.ad_id),
                reject((event) => !Roas().utilities.is_valid_fb_id(event.ad_id))
            )
        ),

        rxmap(louniqby("ad_id")),
        concatMap(identity),
        concatMap((asset) =>
            from(Event.fb_ad_data({ ...asset, user_id, fb_ad_account_id, date })).pipe(
                concatMap(identity),
                rxmap((ad) => ({ ...asset, ...ad }))
            )
        ),
        rxmap((value) => ({ ...value, from: event_from_type, email })),
        rxmap(of),
        rxmap(filter((asset) => asset.name)),
        rxreduce((prev, curr) => [...prev, ...curr]),
        defaultIfEmpty([])
    );
};

exports.Event = Event;
exports.Events = Events;

const shoping_cart_user_docs_map = {
    clickfunnels_webhook: ClickFunnels.user_db_docs,
};

const shoping_cart_ip_docs_with_ads_map = {
    clickfunnels_webhook: ClickFunnels.ip_docs_with_ad_data,
};

const shoping_cart_email_docs_with_ads_map = {
    clickfunnels_webhook: ClickFunnels.email_docs_with_ad_data,
};

Asset = {};

Asset.stats_default_props = () => ({ fbclicks: 0, fbspend: 0, fbmade: 0, fbsales: 0, fbroas: 0, fbleads: 0 });

Asset.stats = (asset) => {
    let { fbclicks, fbspend, fbmade, fbsales, fbroas, fbleads, ...rest } = asset;
    let stats = {
        fbclicks,
        fbspend,
        fbmade,
        fbsales,
        fbroas,
        fbleads,
    };

    return {
        stats,
        ...rest,
    };
};

Asset.ad_stats = ({ user_id, asset, date, fb_ad_account_id }) => {
    let func_name = `Asset:ad_stats`;
    console.log(func_name);

    if (!user_id) return throwError(`error:${func_name}:no user_id`);
    if (!asset) return throwError(`error:${func_name}:no asset`);
    if (!date) return throwError(`error:${func_name}:no date`);
    if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

    let fields = ["actions", "action_values", "spend", "purchase_roas"];

    return rxof(asset).pipe(
        concatMap((asset) =>
            from(Facebook({ user_id }).campaign.insights.get({ date, campaign_id: asset.asset_id, fb_ad_account_id, fields })).pipe(
                concatMap(identity)
            )
        ),
        rxmap(({ asset_id, fbclicks = 0, fbspend = 0, fbmade = 0, fbsales = 0, fbroas = 0, fbleads = 0 }) => ({
            asset_id,
            fbclicks,
            fbspend,
            fbmade,
            fbsales,
            fbroas,
            fbleads,
        })),
        rxmap(Asset.stats),
        rxmap(mergeDeepRight(asset)),
        rxmap(of)
    );
};

const facebook_db_stats_docs = ({ type, date, fb_ad_account_id }) => {
    return from(
        getDocs(
            query(
                collectionGroup(db, "insight"),
                where("date", "==", date),
                where("type", "==", type),
                where("fb_ad_account_id", "==", fb_ad_account_id)
            )
        )
    ).pipe(
        rxmap((assets) => assets.docs.map((asset) => asset.data())),
        rxmap(mod(all)((asset) => ({ ...asset, asset_id: asset.details.asset_id }))),
        rxmap(lokeyby("asset_id"))
    );
};

const payment_processor_stats_docs = ({ type, date, user_id }) => {
    return from(getDocs(query(collection(db, `payment_processor_${type}s`), where("date", "==", date), where("user_id", "==", user_id)))).pipe(
        rxmap((assets) =>
            pipe(
                prop("docs"),
                map((asset) => asset.data()),
                head,
                defaultTo({})
            )(assets)
        ),
        defaultIfEmpty({})
    );
};

const facebook_docs_stats = (docs) =>
    pipe(
        map(pipe(prop("insight"), Asset.stats)),
        lomap((value, asset_id) => ({ ...value, asset_id })),
        lokeyby("asset_id"),
        defaultTo({})
    )(docs);

const facebook_docs_details = (docs) => pipe(map(prop("details")), defaultTo({}))(docs);

const Roas = (params = {}) => {
    let { date, user_id, fb_account_id, fb_access_token } = params;
    const utilities = {
        is_valid_fb_id: (id) => !isNaN(toNumber(id)),

        get_utc_timestamp: (value) => {
            // console.log("get_utc_timestamp");
            if (get("created_at_unix_timestamp")(value)) {
                console.log(get("created_at_unix_timestamp")(value));
                return get("created_at_unix_timestamp")(value);
            }

            if (get("utc_unix_time")(value)) {
                console.log(get("utc_unix_time")(value));
                return get("utc_unix_time")(value);
            }

            console.log(get("unix_datetime")(value));
            return get("unix_datetime")(value);
        },
    };

    const account = {
        reports: {
            campaigns: {
                get: ({ date, fb_ad_account_id }) => {
                    let func_name = `Roas:account:reports:campaigns:get`;
                    console.log(func_name);

                    if (!date) return throwError(`error:${func_name}:no date`);
                    if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                    let facebook_campaigns_insights_docs = facebook_db_stats_docs({ date, type: "campaign", fb_ad_account_id });

                    return from(facebook_campaigns_insights_docs).pipe(
                        concatMap((facebook_campaigns) => {
                            let facebook_campaigns_stats = pipe(facebook_docs_stats)(facebook_campaigns);
                            let facebook_campaigns_details = pipe(facebook_docs_details)(facebook_campaigns);

                            let stats = rxof(facebook_campaigns_stats).pipe(rxmap(values));

                            let campaigns_with_stats = stats.pipe(rxmap(filter(pipe(hasPath(["stats", "fbspend"])))));
                            let campaigns_without_stats = stats.pipe(rxmap(filter(pipe(hasPath(["stats", "fbspend"]), not))));

                            let campaigns_report = campaigns_without_stats.pipe(
                                concatMap(identity),
                                rxmap(mergeDeepRight(Asset.stats_default_props)),
                                rxfilter(pipe(isEmpty, not)),
                                concatMap((asset) => Asset.ad_stats({ user_id, asset, date, fb_ad_account_id })),
                                defaultIfEmpty([]),
                                rxreduce((prev, curr) => [...prev, ...curr]),
                                withLatestFrom(campaigns_with_stats),
                                rxmap(flattenDeep),
                                concatMap(identity),
                                rxmap((asset) => ({
                                    ...asset,
                                    details: facebook_campaigns_details[asset.asset_id],
                                })),
                                rxmap(of),
                                rxreduce((prev, curr) => [...prev, ...curr]),
                                rxmap(lokeyby("asset_id"))
                            );

                            return campaigns_report;
                        }),
                        defaultIfEmpty({}),
                        catchError((error) => rxof(error))
                    );
                },
            },

            adsets: {
                get: ({ date, fb_ad_account_id }) => {
                    let func_name = `Roas:account:reports:adsets:get`;
                    console.log(func_name);

                    if (!date) return throwError(`error:${func_name}:no date`);
                    if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                    let facebook_adsets_insights_docs = facebook_db_stats_docs({ date, type: "adset", fb_ad_account_id });

                    return from(facebook_adsets_insights_docs).pipe(
                        concatMap((facebook_assets) => {
                            // console.log("facebook_assets");
                            // console.log(facebook_assets);

                            let facebook_assets_stats = pipe(facebook_docs_stats)(facebook_assets);
                            let facebook_assets_details = pipe(facebook_docs_details)(facebook_assets);

                            let stats = rxof(facebook_assets_stats).pipe(rxmap(values));

                            let assets_with_stats = stats.pipe(rxmap(filter(pipe(hasPath(["stats", "fbspend"])))));
                            let assets_without_stats = stats.pipe(rxmap(filter(pipe(hasPath(["stats", "fbspend"]), not))));

                            let assets_report = assets_without_stats.pipe(
                                concatMap(identity),
                                rxmap(mergeDeepRight(Asset.stats_default_props)),
                                rxfilter(pipe(isEmpty, not)),
                                concatMap((asset) => Asset.ad_stats({ user_id, asset, date, fb_ad_account_id })),
                                defaultIfEmpty([]),
                                rxreduce((prev, curr) => [...prev, ...curr]),
                                withLatestFrom(assets_with_stats),
                                rxmap(flattenDeep),
                                concatMap(identity),
                                rxmap((asset) => ({
                                    ...asset,
                                    details: facebook_assets_details[asset.asset_id],
                                })),
                                rxmap(of),
                                rxreduce((prev, curr) => [...prev, ...curr]),
                                rxmap(lokeyby("asset_id"))
                            );

                            return assets_report;
                        }),
                        defaultIfEmpty({}),
                        catchError((error) => rxof(error))
                    );
                },
            },

            ads: {
                get: ({ date, fb_ad_account_id }) => {
                    let func_name = `Roas:account:reports:ads:get`;
                    console.log(func_name);

                    if (!date) return throwError(`error:${func_name}:no date`);
                    if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                    let facebook_insights_docs = facebook_db_stats_docs({ date, type: "ad", fb_ad_account_id });

                    return from(facebook_insights_docs).pipe(
                        concatMap((facebook_assets) => {
                            let facebook_assets_stats = pipe(facebook_docs_stats)(facebook_assets);
                            let facebook_assets_details = pipe(facebook_docs_details)(facebook_assets);

                            let stats = rxof(facebook_assets_stats).pipe(rxmap(values));

                            let assets_with_stats = stats.pipe(rxmap(filter(pipe(hasPath(["stats", "fbspend"])))));
                            let assets_without_stats = stats.pipe(rxmap(filter(pipe(hasPath(["stats", "fbspend"]), not))));

                            let assets_report = assets_without_stats.pipe(
                                concatMap(identity),
                                rxmap(mergeDeepRight(Asset.stats_default_props)),
                                rxfilter(pipe(isEmpty, not)),
                                concatMap((asset) => Asset.ad_stats({ user_id, asset, date, fb_ad_account_id })),
                                defaultIfEmpty([]),
                                rxreduce((prev, curr) => [...prev, ...curr]),
                                withLatestFrom(assets_with_stats),
                                rxmap(flattenDeep),
                                concatMap(identity),
                                rxmap((asset) => ({
                                    ...asset,
                                    details: facebook_assets_details[asset.asset_id],
                                })),
                                rxmap(of),
                                rxreduce((prev, curr) => [...prev, ...curr]),
                                rxmap(lokeyby("asset_id"))
                            );

                            return assets_report;
                        }),
                        defaultIfEmpty({}),
                        catchError((error) => rxof(error))
                    );
                },
            },
        },

        stats: {
            range: (stats) => {
                console.log("statsstats");
                console.log(stats);
                // let name = pipe(get(0, "name"))(stats);
                // let campaign_id = pipe(get(0, "campaign_id"))(stats);
                // let adset_id = pipe(get(0, "adset_id"))(stats);
                // let ad_id = pipe(get(0, "ad_id"))(stats);

                // console.log("stats");
                // console.log(stats);

                let fbclicks = pipe(get(all, "fbclicks"), map(flipNumOrDefault(0)), sum)(stats);
                // let roasclicks = pipe(get(all, "fbclicks"), map(flipNumOrDefault(0)), sum)(stats);

                let fbcustomers = pipe(get(all, "fbsales"), map(flipNumOrDefault(0)), sum)(stats);
                // let roascustomers = pipe(get(all, "roascustomers"), map(flipNumOrDefault(0)), sum)(stats);

                let fbleads = pipe(get(all, "fbleads"), map(flipNumOrDefault(0)), sum)(stats);
                // let roasleads = pipe(get(all, "roasleads"), map(flipNumOrDefault(0)), sum)(stats);

                let fbrevenue = pipe(get(all, "fbmade"), map(flipNumOrDefault(0)), sum, (value) => raToFixed(value, 3), toNumber)(stats);
                // let roasrevenue = pipe(get(all, "roasrevenue"), map(flipNumOrDefault(0)), sum, (value) => raToFixed(value, 3), toNumber)(stats);

                let fbsales = pipe(get(all, "fbsales"), map(flipNumOrDefault(0)), sum)(stats);
                // let roassales = pipe(get(all, "roassales"), map(flipNumOrDefault(0)), sum)(stats);

                let fbspend = pipe(get(all, "fbspend"), map(flipNumOrDefault(0)), sum, (value) => raToFixed(value, 3), toNumber)(stats);
                // let roasspend = pipe(get(all, "fbspend"), map(flipNumOrDefault(0)), sum, (value) => raToFixed(value, 3), toNumber)(stats);

                let fbroas = toNumber(raToFixed(numOrDefault(fbrevenue / fbspend, 0), 3));
                // let roas = toNumber(raToFixed(numOrDefault(roasrevenue / fbspend, 0), 3));

                let fbcostpersale = toNumber(raToFixed(numOrDefault(fbspend / fbsales, fbspend), 3));
                // let roascostpersale = toNumber(raToFixed(numOrDefault(fbspend / roassales, fbspend), 3));

                let fbcostpercustomer = toNumber(raToFixed(numOrDefault(fbspend / fbsales, fbspend), 3));
                // let roascostpercustomer = toNumber(raToFixed(numOrDefault(fbspend / roascustomers, fbspend), 3));

                let fbcostperlead = toNumber(raToFixed(numOrDefault(fbspend / fbleads, fbspend), 3));
                // let roascostperlead = toNumber(raToFixed(numOrDefault(fbspend / roasleads, fbspend), 3));

                let fbaverageordervalue = toNumber(raToFixed(numOrDefault(fbrevenue / fbsales, 0), 3));
                // let roasaverageordervalue = toNumber(raToFixed(numOrDefault(roasrevenue / roascustomers, 0), 3));

                let fbmargin = toNumber(raToFixed(numOrDefault(fbaverageordervalue - fbcostpersale, 0), 3));
                // let roasmargin = toNumber(raToFixed(numOrDefault(roasaverageordervalue - roascostpersale, 0), 3));

                // let order_items = pipe(get(all, "order_items"), reject(isUndefined), map(values), flattenDeep)(stats);

                let payload = {
                    // name,
                    // campaign_id,
                    // adset_id,
                    // ad_id,

                    fbclicks,
                    // roasclicks,

                    fbsales,
                    // roassales,

                    // fbcustomers,
                    // roascustomers,

                    // fbleads,
                    // roasleads,

                    // fbrevenue,
                    // roasrevenue,

                    // fbspend,
                    // roasspend,

                    // fbroas,
                    // roas,

                    // fbcostpersale,
                    // roascostpersale,

                    // fbcostperlead,
                    // roascostperlead,

                    // fbcostpercustomer,
                    // roascostpercustomer,

                    // fbaverageordervalue,
                    // roasaverageordervalue,

                    // fbmargin,
                    // roasmargin,

                    // order_items,
                };

                console.log(payload);

                return payload;
            },

            update: async ({ date, fb_ad_account_id } = {}) => {
                let func_name = `Roas:account:stats:update`;
                console.log(func_name);

                if (!date) return throwError(`error:${func_name}:no date`);
                if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);

                let customer_docs = payment_processor_stats_docs({ type: "campaign", date, user_id });
                let campaigns_report = account.reports.campaigns.get({ date, fb_ad_account_id });
                let adsets_report = account.reports.adsets.get({ date, fb_ad_account_id });
                let ads_report = account.reports.ads.get({ date, fb_ad_account_id });

                return zip([customer_docs, campaigns_report, adsets_report, ads_report]).pipe(
                    concatMap(([customers, campaigns, adsets, ads]) => {
                        let report_id = fb_ad_account_id + date.split("-").join("");
                        let now = new Date().getTime();

                        customers = pipe(prop("customers"), defaultTo({}))(customers);

                        // return rxof(customers);

                        // console.log("campaigns");
                        campaigns = pipe(
                            // pipeLog,
                            identity,
                            values,
                            lochunk(500),
                            lomap((assets, chunk_id) => {
                                return {
                                    campaigns: assets,
                                    type: "campaigns",
                                    user_id,
                                    date,
                                    fb_ad_account_id,
                                    updated_at: now,
                                    report_id,
                                    doc_id: `${fb_ad_account_id}campaigns${chunk_id}${date.split("-").join("")}`,
                                };
                            }),
                            defaultTo([])
                        )(campaigns);

                        // console.log("adsets");
                        adsets = pipe(
                            // pipeLog,
                            identity,
                            values,
                            lochunk(500),
                            lomap((assets, chunk_id) => {
                                return {
                                    adsets: assets,
                                    type: "adsets",
                                    user_id,
                                    date,
                                    fb_ad_account_id,
                                    updated_at: now,
                                    report_id,
                                    doc_id: `${fb_ad_account_id}adsets${chunk_id}${date.split("-").join("")}`,
                                };
                            }),
                            defaultTo([])
                        )(adsets);

                        // console.log("ads");
                        ads = pipe(
                            // pipeLog,
                            identity,
                            values,
                            lochunk(500),
                            lomap((assets, chunk_id) => {
                                return {
                                    ads: assets,
                                    type: "ads",
                                    user_id,
                                    date,
                                    fb_ad_account_id,
                                    updated_at: now,
                                    report_id,
                                    doc_id: `${fb_ad_account_id}ads${chunk_id}${date.split("-").join("")}`,
                                };
                            }),
                            defaultTo([])
                        )(ads);

                        let customers_payload = {
                            customers,
                            type: "customers",
                            user_id,
                            date,
                            fb_ad_account_id,
                            updated_at: now,
                            report_id,
                            doc_id: `${fb_ad_account_id}customers${date.split("-").join("")}`,
                        };

                        let report_details = {
                            type: "details",
                            user_id,
                            date,
                            fb_ad_account_id,
                            updated_at: now,
                            report_id,
                            doc_id: `${fb_ad_account_id}details${date.split("-").join("")}`,
                        };

                        let campaigns_doc = from(campaigns).pipe(
                            concatMap((chunk) => {
                                return from(setDoc(doc(db, "reports", chunk.doc_id), chunk, { merge: false })).pipe(rxmap(() => chunk.doc_id));
                            }),
                            tap(() => console.log("campaigns")),
                            rxmap(pipeLog),
                            rxmap(of),
                            rxreduce((prev, curr) => [...prev, ...curr]),
                            defaultIfEmpty([])
                        );

                        let adsets_doc = from(adsets).pipe(
                            concatMap((chunk) => {
                                return from(setDoc(doc(db, "reports", chunk.doc_id), chunk, { merge: false })).pipe(rxmap(() => chunk.doc_id));
                            }),
                            tap(() => console.log("adsets")),
                            rxmap(pipeLog),
                            rxmap(of),
                            rxreduce((prev, curr) => [...prev, ...curr]),
                            defaultIfEmpty([])
                        );

                        let ads_doc = from(ads).pipe(
                            concatMap((chunk) => {
                                return from(setDoc(doc(db, "reports", chunk.doc_id), chunk, { merge: false })).pipe(rxmap(() => chunk.doc_id));
                            }),
                            tap(() => console.log("ads")),
                            rxmap(pipeLog),
                            rxmap(of),
                            rxreduce((prev, curr) => [...prev, ...curr]),
                            defaultIfEmpty([])
                        );

                        let customers_doc = from(setDoc(doc(db, "reports", customers_payload.doc_id), customers_payload, { merge: false })).pipe(
                            rxmap(() => customers_payload.doc_id),
                            tap(() => console.log("customers")),
                            rxmap(pipeLog)
                        );

                        let details_doc = from(setDoc(doc(db, "reports", report_details.doc_id), report_details, { merge: false })).pipe(
                            rxmap(() => report_details.doc_id),
                            tap(() => console.log("details")),
                            rxmap(pipeLog)
                        );

                        return zip([details_doc, customers_doc, campaigns_doc, adsets_doc, ads_doc]).pipe(
                            tap(() => console.log(`saved report for date ${date}`)),
                            rxmap(flatten)
                        );
                    }),
                    rxfilter(pipe(isEmpty, not)),
                    defaultIfEmpty([])
                );
            },
        },
    };

    let user = {
        ad_data: ({ user, shopping_cart_id, email, user_id, fb_ad_account_id, date }) => {
            let func_name = `Roas:customer:ad_data`;
            console.log(`${func_name}`);

            console.log(`user:ad_data:shopping_cart_id`, shopping_cart_id);

            if (!user) return throwError(`error:${func_name}:no user`);
            if (!shopping_cart_id) return throwError(`error:${func_name}:no shopping_cart_id`);
            if (!date) return throwError(`error:${func_name}:no date`);

            let emails = uniq([email, toLower(email), toUpper(email)]);

            let fingerprint_id = user.user_id;

            let customer_ids = pipe(uniq, lofilter(anyPass([isEmail, isIPv4, isIPv6])), get_customer_ids_type)([...user.ids, ...emails]);

            let ip_ids = pipe(
                filter((value) => value.type == "ipv4" || value.type == "ipv6"),
                map(prop("id"))
            )(customer_ids);

            let email_ids = pipe(
                filter((value) => value.type == "email"),
                map(prop("id"))
            )(customer_ids);

            let user_events_query = query(collection(db, "users", fingerprint_id, "evts"));

            let user_events_docs = from(getDocs(user_events_query)).pipe(
                rxfilter((value) => !isUndefined(value)),
                rxmap((data) => data.docs.map((doc) => doc.data())),
                rxmap(pipeLog)
            );

            let shopping_cart_ip_docs = shoping_cart_ip_docs_with_ads_map[shopping_cart_id]({
                email,
                ip_ids,
                user_id,
                fb_ad_account_id,
                date,
            });

            let shopping_cart_email_docs = shoping_cart_email_docs_with_ads_map[shopping_cart_id]({
                email,
                email_ids,
                user_id,
                fb_ad_account_id,
                date,
            });

            let user_events_response = user_events_docs.pipe(
                concatMap((events) => Events.ad_data({ events, event_from_type: "user_events_response", email, fb_ad_account_id, user_id, date })),
                rxmap(pipeLog)
            );

            let shopping_cart_ip_response = shopping_cart_ip_docs.pipe(
                concatMap((events) =>
                    Events.ad_data({ events, event_from_type: `${shopping_cart_id}_ip_response`, email, fb_ad_account_id, user_id, date })
                )
            );

            let shopping_cart_email_response = shopping_cart_email_docs.pipe(
                concatMap((events) =>
                    Events.ad_data({ events, event_from_type: `${shopping_cart_id}_email_response`, email, fb_ad_account_id, user_id, date })
                )
            );

            return zip([user_events_response, shopping_cart_ip_response, shopping_cart_email_response]).pipe(
                rxmap(pipeLog),
                rxmap(flatten),
                rxmap(sortBy(prop("timestamp"))),
                defaultIfEmpty([])
            );
        },

        ad_data_from_email: ({ email, shopping_cart_id, fb_ad_account_id, date }) => {
            let func_name = "Roas:customer:ad_data_from_email";
            console.log(func_name);

            if (!email) return throwError(`error:${func_name}:no email`);
            if (!shopping_cart_id) return throwError(`error:${func_name}:shopping_cart_id `);
            if (!fb_ad_account_id) return throwError(`error:${func_name}:no fb_ad_account_id`);
            if (!date) return throwError(`error:${func_name}:no date`);

            let emails = uniq([email, toLower(email), toUpper(email)]);
            let users_email_query = query(collection(db, "users"), where("ids", "array-contains-any", emails));

            let users_email_docs = from(getDocs(users_email_query)).pipe(
                rxmap((data) => data.docs.map((doc) => ({ ...doc.data(), user_id: doc.id })))
            );

            let shopping_cart_email_docs = shoping_cart_user_docs_map[shopping_cart_id]({ emails, user_id });

            return zip([users_email_docs, shopping_cart_email_docs]).pipe(
                rxmap(pipeLog),
                rxmap(flatten),
                rxfilter((value) => !isEmpty(value)),
                rxmap(louniqby("user_id")),
                concatMap(identity),
                concatMap((usr) => user.ad_data({ user: usr, shopping_cart_id, email, user_id, fb_ad_account_id, date })),
                rxreduce((prev, curr) => [...prev, ...curr]),
                rxmap(sortBy(prop("timestamp"))),
                rxmap(logroupby("ad_id")),
                rxmap(mod(all)(last)),
                rxmap(pipe(values, flattenDeep)),
                rxmap(louniqby("ad_id")),
                defaultIfEmpty([])
            );
        },
    };

    const metrics = {
        get: () => {
            let func_name = "Roas:metrics:get";
            console.log(func_name);

            let keap = Keap.orders({ date, user_id, fb_account_id, fb_access_token });
            let keap_orders = keap.get();
            let cart_by_customer = keap_orders.pipe(concatMap(keap.cart_items_grouped_by_emails));
            let ad_ids_by_customer = keap_orders.pipe(concatMap(keap.customers_with_ad_ids), rxmap(lokeyby("email")));

            return zip(cart_by_customer, ad_ids_by_customer).pipe(
                rxmap(([left, right]) => mergeDeepRight(left, right)),
                rxmap((value) => {
                    console.log(`${func_name}:get_group_metrics`);

                    const get_group_metrics = (data) => ({
                        roasrevenue: pipe(map(prop("cart")), flatten, reject(isUndefined), sum)(data),
                        roascustomers: pipe(map(prop("cart")), size)(data),
                        roassales: pipe(map(prop("cart")), flatten, size)(data),
                        order_items: data,
                    });

                    let grouped_by_campaigns = pipe(
                        logroupby("campaign_id"),
                        mod(all)(
                            pipe(
                                get_group_metrics,
                                (value) => ({ ...value, campaign_id: prop("campaign_id", head(value.order_items)) }),
                                objOf("stats")
                            )
                        )
                    )(value);

                    let { undefined: undefined_campaigns, ...rest_of_campaigns } = grouped_by_campaigns;

                    if (undefined_campaigns) {
                        undefined_campaigns = { ...undefined_campaigns, stats: { ...undefined_campaigns.stats, campaign_id: "" } };
                    } else {
                        undefined_campaigns = {};
                    }

                    grouped_by_campaigns = { ...rest_of_campaigns, undefined_campaigns };

                    let grouped_by_adsets = pipe(
                        logroupby("adset_id"),
                        mod(all)(
                            pipe(
                                get_group_metrics,
                                (value) => ({
                                    ...value,
                                    campaign_id: prop("campaign_id", head(value.order_items)),
                                    adset_id: prop("adset_id", head(value.order_items)),
                                }),
                                objOf("stats")
                            )
                        )
                    )(value);

                    let { undefined: undefined_adsets, ...rest_of_adsets } = grouped_by_adsets;
                    undefined_adsets = { ...undefined_adsets, stats: { ...undefined_adsets.stats, campaign_id: "", adset_id: "" } };
                    grouped_by_adsets = { ...rest_of_adsets, undefined_adsets };

                    let grouped_by_ads = pipe(
                        logroupby("ad_id"),
                        mod(all)(
                            pipe(
                                get_group_metrics,
                                (value) => ({
                                    ...value,
                                    campaign_id: prop("campaign_id", head(value.order_items)),
                                    adset_id: prop("adset_id", head(value.order_items)),
                                    ad_id: prop("ad_id", head(value.order_items)),
                                }),
                                objOf("stats")
                            )
                        )
                    )(value);

                    let { undefined: undefined_ads, ...rest_of_ads } = grouped_by_ads;
                    undefined_ads = {
                        ...undefined_ads,
                        stats: { ...undefined_ads.stats, campaign_id: "", adset_id: "", ad_id: "" },
                    };

                    grouped_by_ads = { ...rest_of_ads, undefined_ads };

                    let payload = {
                        campaigns: grouped_by_campaigns,
                        adsets: grouped_by_adsets,
                        ads: grouped_by_ads,
                    };

                    return payload;
                })
            );
        },
    };

    return { user_id, metrics, utilities, user, account };
};

// Roas.customer.ad_data_from_email()

let user_id = "aobouNIIRJMSjsDs2dIXAwEKmiY2";
// let user_id = "8O2p0Rf8jlhWprh9zxFNtdeSUFA3";
let date = "2022-04-01";
let now = new Date().getTime();

let report_one_query = query(doc(db, "reports", "156051941066130720220416"));
let report_one_response = from(getDoc(report_one_query)).pipe(rxmap((doc) => doc.data()));
let report_two_query = query(doc(db, "reports", "156051941066130720220415"));
let report_two_response = from(getDoc(report_two_query)).pipe(rxmap((snapshot) => snapshot.docs.map((doc) => doc.data())));

let dates = ["2022-04-26"];

const Report = {
    utilities: {
        attribution_timestamp: (attribution_window = 7, date) => {
            var attribution_date = moment(date, "YYYY-MM-DD").subtract(attribution_window, "days").startOf("day");
            let timestamp = attribution_date.unix();
            return timestamp;
        },

        toFixed: curry((fixed, num) => {
            var re = new RegExp("^-?\\d+(?:.\\d{0," + (fixed || -1) + "})?");
            return num.toString().match(re)[0];
        }),

        numOrZero: (value) => {
            return isNaN(value) || !isFinite(value) || isUndefined(value) ? 0 : toNumber(value);
        },

        numOrDefault: curry((value, defaultValue) => {
            return Report.utilities.numOrZero(value) == 0 ? defaultValue : Report.utilities.numOrZero(value);
        }),
    },

    defaults: {
        stats: {
            roas: () => {
                return {
                    roasclicks: 0,
                    roassales: 0,
                    roascustomers: 0,
                    roasleads: 0,
                    roasrevenue: 0,
                    roasspend: 0,
                };
            },
            all: () => {
                return {
                    fbclicks: 0,
                    fbleads: 0,
                    fbsales: 0,
                    roassales: 0,
                    roasrevenue: 0,
                    roascustomers: 0,
                    fbspend: 0,
                    fbmade: 0,
                    fbroas: 0,
                    roas: 0,
                    fbcustomers: 0,
                    fbrevenue: 0,
                    roasclicks: 0,
                    roasleads: 0,
                    roasspend: 0,
                    fbmargin: 0,
                    roasmargin: 0,
                    fbcostpersale: 0,
                    roascostpersale: 0,
                    fbcostperlead: 0,
                    roascostperlead: 0,
                    fbcostpercustomer: 0,
                    roascostpercustomer: 0,
                    fbaverageordervalue: 0,
                    roasaverageordervalue: 0,
                    fbaveragesalespercustomer: 0,
                    roasaveragesalespercustomer: 0,
                };
            },
        },
    },

    customers: {
        get: (report) => {
            return pipe(
                get("customers"),
                values,
                mod(all)((order) => {
                    let result = mod(
                        "ads",
                        all
                    )((ad) => ({
                        ...ad,
                        stats: order.stats,
                        cart: order.cart,
                        email: toLower(order.email),
                        report_date: report.details.date,
                        // type: "campaign",
                    }))(order);

                    return result;
                }),
                get(all, "ads"),
                flatten
            )(report);
        },
    },

    customer: {
        normalize: (customer) => {
            let func_name = `Report:customer:normalize`;
            console.log(func_name);

            let email = pipe(head, get("email"), toLower)(customer);
            let ads = pipe(louniqby("ad_id"), mod(all)(omit(["cart", "stats"])))(customer);
            let cart = pipe(louniqby("report_date"), get(all, "cart"), flatten)(customer);
            let stats = {
                roasrevenue: pipe(louniqby("report_date"), get(all, "stats", "roasrevenue"), sum)(customer),
                roassales: pipe(louniqby("report_date"), get(all, "stats", "roassales"), sum)(customer),
            };

            return pipe(
                mod(all)((order) => ({
                    ...order,
                    cart,
                    email,
                    stats,
                }))
            )(ads);
        },
    },

    ads: {
        stats: (stats) => {
            let func_name = `Report:ads:stats`;
            console.log(func_name);

            return {
                roassales: pipe(get(all, "roassales"), sum)(stats),
                roasrevenue: pipe(get(all, "roasrevenue"), sum)(stats),
                roascustomers: pipe(size)(stats),
            };
        },

        get: (customer) => {
            return get("ads")(customer);
        },

        // normalize: (customer) => {
        //     return pipe(mod("ads", all)((ad) => ({ ...ad, cart: customer.cart, email: customer.email, stats: customer.stats })))(customer);
        // },

        attribution: {
            timestamp: (attribution_window, date) => {
                return Report.utilities.attribution_timestamp(attribution_window, date);
            },

            inside: curry((attribution_window, order) => {
                return order.timestamp > Report.ads.attribution.timestamp(attribution_window, order.report_date);
            }),
        },
    },

    asset: {
        stats: {
            roas: (stats) => {
                let func_name = `Report:asset:stats:roas`;
                console.log(func_name);
                let { fbspend, roasrevenue } = stats;
                return numOrZero(roasrevenue / fbspend);
            },

            all: (stats) => {
                let func_name = `Report:asset:stats:all`;
                console.log(func_name);

                let { fbclicks, fbsales, fbleads, fbmade, fbspend, roassales, roascustomers, roasrevenue } = stats;

                let fbcustomers = fbsales;
                let fbrevenue = fbmade;

                let roasclicks = fbclicks;
                let roasleads = fbleads;
                let roasspend = fbspend;

                let roas = pipe(Report.utilities.numOrZero)(roasrevenue / fbspend);
                let fbroas = pipe(Report.utilities.numOrZero)(fbrevenue / fbspend);

                let fbcostpersale = pipe(Report.utilities.numOrZero)(fbspend / fbsales);
                let roascostpersale = pipe(Report.utilities.numOrZero)(fbspend / roassales);

                let fbcostperlead = pipe(Report.utilities.numOrZero)(fbspend / fbleads);
                let roascostperlead = pipe(Report.utilities.numOrZero)(fbspend / roasleads);

                let fbcostpercustomer = pipe(Report.utilities.numOrZero)(fbspend / roascustomers);
                let roascostpercustomer = pipe(Report.utilities.numOrZero)(fbspend / roascustomers);

                let fbaverageordervalue = pipe(Report.utilities.numOrZero)(fbrevenue / fbsales);
                let roasaverageordervalue = pipe(Report.utilities.numOrZero)(roasrevenue / roascustomers);

                let fbmargin = fbaverageordervalue - fbcostpersale;
                let roasmargin = roasaverageordervalue - roascostpersale;

                let fbaveragesalespercustomer = pipe(Report.utilities.numOrZero)(fbsales / fbcustomers);
                let roasaveragesalespercustomer = pipe(Report.utilities.numOrZero)(roassales / roascustomers);

                return {
                    fbclicks: pipe(defaultTo(0))(fbclicks),
                    fbleads: pipe(defaultTo(0))(fbleads),
                    fbsales: pipe(defaultTo(0))(fbsales),
                    roassales: pipe(defaultTo(0))(roassales),
                    roasrevenue: pipe(defaultTo(0))(roasrevenue),
                    roascustomers: pipe(defaultTo(0))(roascustomers),
                    fbspend: pipe(defaultTo(0))(fbspend),
                    fbmade: pipe(defaultTo(0))(fbmade),
                    fbroas: pipe(defaultTo(0))(fbroas),
                    roas: pipe(defaultTo(0))(roas),
                    fbcustomers: pipe(defaultTo(0))(fbcustomers),
                    fbrevenue: pipe(defaultTo(0))(fbrevenue),
                    roasclicks: pipe(defaultTo(0))(roasclicks),
                    roasleads: pipe(defaultTo(0))(roasleads),
                    roasspend: pipe(defaultTo(0))(roasspend),
                    fbmargin: pipe(defaultTo(0))(fbmargin),
                    roasmargin: pipe(defaultTo(0))(roasmargin),
                    fbcostpersale: pipe(defaultTo(0))(fbcostpersale),
                    roascostpersale: pipe(defaultTo(0))(roascostpersale),
                    fbcostperlead: pipe(defaultTo(0))(fbcostperlead),
                    roascostperlead: pipe(defaultTo(0))(roascostperlead),
                    fbcostpercustomer: pipe(defaultTo(0))(fbcostpercustomer),
                    roascostpercustomer: pipe(defaultTo(0))(roascostpercustomer),
                    fbaverageordervalue: pipe(defaultTo(0))(fbaverageordervalue),
                    roasaverageordervalue: pipe(defaultTo(0))(roasaverageordervalue),
                    fbaveragesalespercustomer: pipe(defaultTo(0))(fbaveragesalespercustomer),
                    roasaveragesalespercustomer: pipe(defaultTo(0))(roasaveragesalespercustomer),
                };
            },
        },
    },

    assets: {
        stats: (stats) => {
            let func_name = `Report:assets:stats`;
            console.log(func_name);

            let fbspend = pipe(get(all, "fbspend"), sum)(stats);
            let fbmade = pipe(get(all, "fbmade"), sum)(stats);
            return {
                fbclicks: pipe(get(all, "fbclicks"), sum)(stats),
                fbleads: pipe(get(all, "fbleads"), sum)(stats),
                fbsales: pipe(get(all, "fbsales"), sum)(stats),
                roassales: 0,
                roasrevenue: 0,
                roascustomers: 0,
                fbspend,
                fbmade,
                fbroas: numOrZero(fbmade / fbspend),
                roas: 0,
            };
        },

        campaigns: {
            details: (assets) => {
                console.log("Report:assets:campaigns:details");

                return {
                    campaign_id: pipe(head, get("campaign_id"))(assets),
                    campaign_name: pipe(head, get("campaign_name"))(assets),
                    asset_id: pipe(head, get("campaign_id"))(assets),
                    asset_name: pipe(head, get("campaign_name"))(assets),
                };
            },
            values: (reports) => {
                return pipe(
                    get(all, "campaigns"),
                    mod(all)(values),
                    flatten,
                    mod(all)(({ details, ...asset }) => ({ ...details, ...asset })),
                    mod(all)(pick(["campaign_id", "campaign_name", "stats"])),
                    mod(all, "stats")((stats) => ({ ...stats, ...Report.defaults.stats.roas() })),
                    logroupby("campaign_id"),
                    mod(all)((assets) => ({
                        campaign_id: pipe(head, get("campaign_id"))(assets),
                        campaign_name: pipe(head, get("campaign_name"))(assets),
                        asset_id: pipe(head, get("campaign_id"))(assets),
                        asset_name: pipe(head, get("campaign_name"))(assets),
                        stats: Report.assets.stats(pipe(get(all, "stats"))(assets)),
                    })),
                    mod(all)((asset) => ({ ...asset, type: "campaign" }))
                )(reports);
            },
        },

        adsets: {
            details: (assets) => {
                console.log("Report:assets:adset:details");

                return {
                    campaign_id: pipe(head, get("campaign_id"))(assets),
                    campaign_name: pipe(head, get("campaign_name"))(assets),
                    adset_id: pipe(head, get("adset_id"))(assets),
                    adset_name: pipe(head, get("adset_name"))(assets),
                    asset_id: pipe(head, get("adset_id"))(assets),
                    asset_name: pipe(head, get("adset_name"))(assets),
                };
            },
            values: (reports) => {
                return pipe(
                    get(all, "adsets"),
                    mod(all)(values),
                    flatten,
                    mod(all)(({ details, ...asset }) => ({ ...details, ...asset })),
                    mod(all)(pick(["campaign_id", "campaign_name", "adset_id", "adset_name", "stats"])),
                    mod(all, "stats")((stats) => ({ ...stats, ...Report.defaults.stats.roas() })),
                    logroupby("adset_id"),
                    mod(all)((assets) => ({
                        campaign_id: pipe(head, get("campaign_id"))(assets),
                        campaign_name: pipe(head, get("campaign_name"))(assets),
                        adset_id: pipe(head, get("adset_id"))(assets),
                        adset_name: pipe(head, get("adset_name"))(assets),
                        asset_id: pipe(head, get("adset_id"))(assets),
                        asset_name: pipe(head, get("adset_name"))(assets),
                        stats: Report.assets.stats(pipe(get(all, "stats"))(assets)),
                    })),
                    mod(all)((asset) => ({ ...asset, type: "adset" }))
                )(reports);
            },
        },

        ads: {
            details: (assets) => {
                console.log("Report:assets:ads:details");
                return {
                    campaign_id: pipe(head, get("campaign_id"))(assets),
                    campaign_name: pipe(head, get("campaign_name"))(assets),
                    adset_name: pipe(head, get("adset_name"))(assets),
                    adset_id: pipe(head, get("adset_id"))(assets),
                    ad_id: pipe(head, get("ad_id"))(assets),
                    ad_name: pipe(head, get("ad_name"))(assets),
                    asset_id: pipe(head, get("ad_id"))(assets),
                    asset_name: pipe(head, get("ad_name"))(assets),
                };
            },
            values: (reports) => {
                return pipe(
                    get(all, "ads"),
                    mod(all)(values),
                    flatten,
                    mod(all)(({ details, ...asset }) => ({ ...details, ...asset })),
                    mod(all)(pick(["campaign_id", "campaign_name", "adset_id", "adset_name", "ad_id", "ad_name", "stats"])),
                    mod(all, "stats")((stats) => ({ ...stats, ...Report.defaults.stats.roas() })),
                    logroupby("ad_id"),
                    mod(all)((assets) => ({
                        campaign_id: pipe(head, get("campaign_id"))(assets),
                        campaign_name: pipe(head, get("campaign_name"))(assets),
                        adset_id: pipe(head, get("adset_id"))(assets),
                        adset_name: pipe(head, get("adset_name"))(assets),
                        ad_id: pipe(head, get("ad_id"))(assets),
                        ad_name: pipe(head, get("ad_name"))(assets),
                        asset_id: pipe(head, get("ad_id"))(assets),
                        asset_name: pipe(head, get("ad_name"))(assets),
                        stats: Report.assets.stats(pipe(get(all, "stats"))(assets)),
                    })),
                    mod(all)((asset) => ({ ...asset, type: "ad" }))
                )(reports);
            },
        },

        details: (type, assets) => {
            console.log("Report:assets:details");

            return pipe(Report.assets[type].details)(assets);
        },

        customers: {
            values: (attribution_window, reports) => {
                return pipe(
                    // get(all, type),
                    // flatten,
                    // pipeLog,
                    lomap(Report.customers.get),
                    // pipeLog,
                    pipe(mod(all)(lofilter(Report.ads.attribution.inside(attribution_window)))),
                    flatten,
                    mod(all, "email")(toLower),
                    logroupby("email"),
                    values,
                    mod(all)(Report.customer.normalize),
                    flatten,
                    mod(all)(
                        pick([
                            "adset_name",
                            "ad_name",
                            "asset_id",
                            "asset_name",
                            "email",
                            "adset_id",
                            "ad_id",
                            "timestamp",
                            "name",
                            "campaign_name",
                            "campaign_id",
                            "cart",
                            "stats",
                            "type",
                        ])
                    ),
                    loorderby(["timestamp"], ["asc"])
                )(reports);
            },

            groupings: {
                by: {
                    product: curry((type, asset_ids, customers) => {
                        let func_name = "Report:assets:customers:grouping:by:product";
                        console.log(func_name);
                        let type_singularize = pluralize.singular(type);
                        let asset_id_prop_getter = `${type_singularize}_id`;

                        return pipe(
                            identity,
                            get(matching({ [asset_id_prop_getter]: (asset_id) => asset_ids.includes(asset_id) })),
                            mod(all)(({ cart, stats, ...rest }) =>
                                pipe(mod(all)(({ name, ...item }) => ({ product_name: name, ...item, ...rest })))(cart)
                            ),
                            flatten,
                            logroupby("product_name")
                        )(customers);
                    }),
                },
            },
        },
    },

    stats: (report) => {
        let spend = pipe(get(all, "stats", "fbspend"), values, sum)(report);
        let made = pipe(get(all, "stats", "roasrevenue"), values, sum)(report);
        let sales = pipe(get(all, "stats", "roassales"), values, sum)(report);
        let customers = pipe(get(all, "stats", "roascustomers"), values, sum)(report);
        let roas = pipe(Report.utilities.numOrZero)(made / spend);
        return {
            spend,
            made,
            sales,
            roas,
            customers,
        };
    },

    get: (type, attribution_window, reports) => {
        let type_id_getter = `${pluralize.singular(type)}_id`;
        let type_plural = pluralize(type);

        return pipe(
            logroupby("email"),
            lomap(last),
            logroupby(type_id_getter),
            mod(all)((assets) => ({
                ...Report.assets.details(type_plural, assets),
                orders: assets,
                stats: Report.ads.stats(get(all, "stats")(assets)),
            })),
            mergeDeepRight(Report.assets[type_plural]["values"](reports)),
            mod(all)((asset) => ({
                ...asset,
                stats: Report.asset.stats.all(pipe(get("stats"))(asset)),
            }))
            // pipeLog
            // pipeLog
            // (report) => ({
            //     ...report,
            //     stats: Report.stats(report),
            // }),
            // pipeLog
        )(Report.assets.customers.values(attribution_window, reports));
    },
};

exports.Report = Report;

// from(dates).pipe(
//     concatMap((date) => {
//         let report_query = query(collection(db, "reports"), where("date", "==", date), where("user_id", "==", "aobouNIIRJMSjsDs2dIXAwEKmiY2"));
//         let report_response = from(getDocs(report_query)).pipe(
//             rxmap((snapshot) => snapshot.docs.map((doc) => doc.data())),
//             rxmap((payload) => ({
//                 campaigns: pipe(get(matching({ type: "campaigns" })), get(all, "campaigns"), flatten)(payload),
//                 // adsets: pipe(get(matching({ type: "adsets" })), get(all, "adsets"), flatten)(payload),
//                 // ads: pipe(get(matching({ type: "ads" })), get(all, "ads"), flatten)(payload),
//                 customers: pipe(get(matching({ type: "customers" })), get(all, "customers"), mod(all)(values), flatten)(payload),
//                 details: pipe(get(matching({ type: "details" })), head)(payload),
//             })),
//             rxmap(of)
//         );
//         return report_response;
//     }),
//     rxreduce((prev, curr) => [...prev, ...curr])
// )
// .subscribe((reports) => {
//     console.log("reportsare");
//     console.log(reports);
//     let customers = pipe(
//         loorderby(["timestamp"], ["desc"]),
//         logroupby("email"),
//         mod(all)(head),
//         values,
//         mod(all)(({ cart, stats, ...rest }) => mod(all)(({ name, ...item }) => ({ product_name: name, ...item, ...rest }))(cart)),
//         flatten
//     )(Report.assets.customers.values(7, reports));

//     // console.log(reports);
//     // Report.get("campaigns", 7, reports);
// });

// zip([report_one_response, report_two_response])
//     .pipe(rxmap(([one, two]) => [one, two]))
//     .subscribe((reports) => {
//         console.log("reportsresult");
//         // console.log(reports);

//         let campaigns = pipe(get(all, "campaigns"), mod(all)(values), flatten)(reports);
//         let adsets = pipe(get(all, "adsets"), mod(all)(values), flatten)(reports);
//         let ads = pipe(get(all, "ads"), mod(all)(values), flatten)(reports);
//         let customers = pipe(get(all, "customers"))(reports);

//         // let campaigns_payload = pipe(
//         //     mod(all)(({ details, ...asset }) => ({ ...details, ...asset })),
//         //     mod(all)(pick(["campaign_id", "campaign_name", "stats"])),
//         //     mod(all, "stats")((stats) => ({ ...stats, ...Report.defaults.stats.roas() })),
//         //     logroupby("campaign_id"),
//         //     mod(all)((assets) => ({
//         //         campaign_name: pipe(head, get("campaign_name"))(assets),
//         //         campaign_id: pipe(head, get("campaign_id"))(assets),
//         //         stats: Report.assets.stats(pipe(get(all, "stats"))(assets)),
//         //     })),
//         //     mod(all)((asset) => ({ ...asset, type: "campaign" }))
//         // )(campaigns);

//         // let adsets_payload = pipe(
//         //     mod(all)(({ details, ...asset }) => ({ ...details, ...asset })),
//         //     mod(all, "stats")((stats) => ({ ...stats, ...Report.defaults.stats.roas() })),
//         //     mod(all)(pick(["campaign_id", "campaign_name", "adset_id", "adset_name", "stats"])),
//         //     logroupby("adset_id"),
//         //     mod(all)((assets) => ({
//         //         adset_name: pipe(head, get("adset_name"))(assets),
//         //         adset_id: pipe(head, get("adset_id"))(assets),
//         //         campaign_id: pipe(head, get("campaign_id"))(assets),
//         //         campaign_name: pipe(head, get("campaign_name"))(assets),
//         //         stats: Report.assets.stats(pipe(get(all, "stats"))(assets)),
//         //     })),
//         //     mod(all)((asset) => ({ ...asset, type: "adset" }))
//         // )(adsets);

//         // let ads_payload = pipe(
//         //     mod(all)(({ details, ...asset }) => ({ ...details, ...asset })),
//         //     mod(all, "stats")((stats) => ({ ...stats, ...Report.defaults.stats.roas() })),
//         //     mod(all)(pick(["campaign_id", "campaign_name", "adset_id", "adset_name", "ad_id", "ad_name", "stats"])),
//         //     logroupby("ad_id"),
//         //     mod(all)((assets) => ({
//         //         adset_name: pipe(head, get("adset_name"))(assets),
//         //         adset_id: pipe(head, get("adset_id"))(assets),
//         //         campaign_id: pipe(head, get("campaign_id"))(assets),
//         //         campaign_name: pipe(head, get("campaign_name"))(assets),
//         //         ad_id: pipe(head, get("ad_id"))(assets),
//         //         ad_name: pipe(head, get("ad_name"))(assets),
//         //         stats: Report.assets.stats(pipe(get(all, "stats"))(assets)),
//         //     })),
//         //     mod(all)((asset) => ({ ...asset, type: "ad" }))
//         // )(ads);

//         // let customers_payload = pipe(
//         //     mod(all)(values),
//         //     flatten,
//         //     logroupby("email"),
//         //     values,
//         //     mod(all)(Report.customer.normalize),
//         //     mod(all)(Report.ads.normalize),
//         //     mod(all)(Report.ads.get),
//         //     flatten,
//         //     mod(all)(
//         //         pick([
//         //             "adset_name",
//         //             "ad_name",
//         //             "asset_id",
//         //             "asset_name",
//         //             "email",
//         //             "adset_id",
//         //             "ad_id",
//         //             "timestamp",
//         //             "name",
//         //             "campaign_name",
//         //             "campaign_id",
//         //             "cart",
//         //             "stats",
//         //         ])
//         //     ),
//         //     loorderby(["timestamp"], ["asc"]),
//         //     mod(all, "email")(toLower)
//         // )(customers);

//         pipe(
//             Report.ads.attribution.inside("2022-04-16", 180),
//             logroupby("email"),
//             lomap(head),
//             logroupby("campaign_id"),
//             mod(all)((assets) => ({
//                 orders: assets,
//                 stats: Report.ads.stats(get(all, "stats")(assets)),
//             })),
//             mergeDeepRight(campaigns_payload),
//             mod(matching({ orders: (orders) => !isUndefined(orders) }))((asset) => ({
//                 ...asset,
//                 stats: Report.asset.stats.all(pipe(get("stats"))(asset)),
//             })),
//             pipeLog
//         )(customers_payload);
//     }).subscribe((report) => {
//     console.log("report");
//     pipeLog(report);

//     let stats = pipe(
//         get("customers"),
//         get(all, "stats"),
//         values,
//         mod(all)(({ roasrevenue }) => roasrevenue),
//         sum
//     )(report);

//     // pipeLog(stats);
// });

// from(getDocs(query(collection(db, "reports"), limit(10))))
//     .pipe(
//         rxmap((snapshot) => snapshot.docs.map((doc) => ({ doc_id: doc.id }))),
//         concatMap(identity)
//     )
//     .subscribe((report) => {
//         console.log(report.doc_id);
//         from(deleteDoc(doc(db, "reports", report.doc_id))).subscribe(() => {
//             console.log("deleted report", report.doc_id);
//         });
//     });

// from(getDocs(query(collection(db, "user_events"), limit(10))))
//     .pipe(
//         rxmap((snapshot) => snapshot.docs.map((doc) => doc.data())),
//         concatMap(identity)
//     )
//     .subscribe((report) => {
//         console.log("report");
//         console.log(report);
//         // console.log(report.doc_id);
//         // from(deleteDoc(doc(db, "reports", report.doc_id))).subscribe(() => {
//         //     console.log("deleted report", report.doc_id);
//         // });
//     });

exports.Roas = Roas;
