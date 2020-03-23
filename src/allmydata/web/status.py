
import pprint, itertools, hashlib
import json
from twisted.internet import defer
from twisted.web.resource import Resource
from twisted.python.filepath import FilePath
from twisted.web.template import (
    Element,
    XMLFile,
    tags as T,
    renderer,
    renderElement
)

from allmydata.util import base32, idlib
from allmydata.web.common import (
    getxmlfile,
    abbreviate_time,
    abbreviate_rate,
    abbreviate_size,
    plural,
    compute_rate,
    render_time,
    MultiFormatResource,
    SlotsSequenceElement,
)
from allmydata.interfaces import (
    IUploadStatus,
    IDownloadStatus,
    IPublishStatus,
    IRetrieveStatus,
    IServermapUpdaterStatus
)

class RateAndTimeMixin(object):

    def render_time(self, ctx, data):
        return abbreviate_time(data)

    def render_rate(self, ctx, data):
        return abbreviate_rate(data)

#------------------------------------------------------------------------

# UploadResultsRendererMixin is inherited by status.UploadResultsPage
# (below), and unliked.UploadResultsPage.
class UploadResultsRendererMixin(Element, RateAndTimeMixin):
    # this requires a method named 'upload_results'

    @renderer
    def pushed_shares(self, req, tag):
        d = self.upload_results()
        d.addCallback(lambda res: tag(res.get_pushed_shares()))
        return d

    @renderer
    def preexisting_shares(self, req, tag):
        d = self.upload_results()
        d.addCallback(lambda res: tag(res.get_preexisting_shares()))
        return d

    @renderer
    def sharemap(self, req, tag):
        d = self.upload_results()
        d.addCallback(lambda res: res.get_sharemap())
        def _render(sharemap):
            if sharemap is None:
                return tag("None")
            l = T.ul()
            for shnum, servers in sorted(sharemap.items()):
                server_names = ', '.join([s.get_name() for s in servers])
                l(T.li("%d -> placed on [%s]" % (shnum, server_names)))
            return tag(l)
        d.addCallback(_render)
        return d

    @renderer
    def servermap(self, req, tag):
        d = self.upload_results()
        d.addCallback(lambda res: res.get_servermap())
        def _render(servermap):
            if servermap is None:
                return tag("None")
            l = T.ul()
            for server, shnums in sorted(servermap.items()):
                shares_s = ",".join(["#%d" % shnum for shnum in shnums])
                l(T.li("[%s] got share%s: %s" % (server.get_name(),
                                                 plural(shnums), shares_s)))
            return tag(l)
        d.addCallback(_render)
        return d

    @renderer
    def file_size(self, req, tag):
        d = self.upload_results()
        d.addCallback(lambda res: tag(str(res.get_file_size())))
        return d

    def _get_time(self, name):
        d = self.upload_results()
        d.addCallback(lambda res: str(res.get_timings().get(name)))
        return d

    @renderer
    def time_total(self, req, tag):
        return tag(self._get_time("total"))

    @renderer
    def time_storage_index(self, req, tag):
        return tag(self._get_time("storage_index"))

    @renderer
    def time_contacting_helper(self, req, tag):
        return tag(self._get_time("contacting_helper"))

    @renderer
    def time_cumulative_fetch(self, req, tag):
        return tag(self._get_time("cumulative_fetch"))

    @renderer
    def time_helper_total(self, req, tag):
        return tag(self._get_time("helper_total"))

    @renderer
    def time_peer_selection(self, req, tag):
        return tag(self._get_time("peer_selection"))

    @renderer
    def time_total_encode_and_push(self, req, tag):
        return tag(self._get_time("total_encode_and_push"))

    @renderer
    def time_cumulative_encoding(self, req, tag):
        return tag(self._get_time("cumulative_encoding"))

    @renderer
    def time_cumulative_sending(self, req, tag):
        return tag(self._get_time("cumulative_sending"))

    @renderer
    def time_hashes_and_close(self, req, tag):
        return tag(self._get_time("hashes_and_close"))

    def _get_rate(self, name):
        d = self.upload_results()
        def _convert(r):
            file_size = r.get_file_size()
            duration = r.get_timings().get(name)
            return str(compute_rate(file_size, duration))
        d.addCallback(_convert)
        return d

    @renderer
    def rate_total(self, req, tag):
        return tag(self._get_rate("total"))

    @renderer
    def rate_storage_index(self, req, tag):
        return tag(self._get_rate("storage_index"))

    @renderer
    def rate_encode(self, req, tag):
        return tag(self._get_rate("cumulative_encoding"))

    @renderer
    def rate_push(self, req, tag):
        return tag(self._get_rate("cumulative_sending"))

    @renderer
    def rate_encode_and_push(self, req, tag):
        d = self.upload_results()
        def _convert(r):
            file_size = r.get_file_size()
            time1 = r.get_timings().get("cumulative_encoding")
            time2 = r.get_timings().get("cumulative_sending")
            if (time1 is None or time2 is None):
                return tag
            else:
                return tag(str(compute_rate(file_size, time1+time2)))
        d.addCallback(_convert)
        return d

    @renderer
    def rate_ciphertext_fetch(self, req, tag):
        d = self.upload_results()
        def _convert(r):
            fetch_size = r.get_ciphertext_fetched()
            duration = r.get_timings().get("cumulative_fetch")
            return tag(str(compute_rate(fetch_size, duration)))
        d.addCallback(_convert)
        return d

#------------------------------------------------------------------------

class UploadStatusPage(MultiFormatResource):

    def __init__(self, data):
        super(UploadStatusPage, self).__init__()
        self.upload_status = data

    def render_HTML(self, req):
        return renderElement(req, UploadStatusElement(self.upload_status))

class UploadStatusElement(UploadResultsRendererMixin):

    loader = XMLFile(FilePath(__file__).sibling("upload-status.xhtml"))

    def __init__(self, upload_status):
        super(UploadStatusElement, self).__init__()
        self.upload_status = upload_status

    def upload_results(self):
        return defer.maybeDeferred(self.upload_status.get_results)

    @renderer
    def results(self, req, tag):
        d = self.upload_results()
        def _got_results(results):
            if results:
                return tag
            return ""
        d.addCallback(_got_results)
        return d

    @renderer
    def started(self, req, tag):
        started_s = render_time(self.upload_status.get_started())
        return started_s

    @renderer
    def si(self, req, tag):
        si_s = base32.b2a_or_none(self.upload_status.get_storage_index())
        if si_s is None:
            si_s = "(None)"
        return si_s

    @renderer
    def helper(self, req, tag):
        return {True: "Yes",
                False: "No"}[self.upload_status.using_helper()]

    @renderer
    def total_size(self, req, tag):
        size = self.upload_status.get_size()
        if size is None:
            return "(unknown)"
        return size

    @renderer
    def progress_hash(self, req, tag):
        progress = self.upload_status.get_progress()[0]
        # TODO: make an ascii-art bar
        return "%.1f%%" % (100.0 * progress)

    @renderer
    def progress_ciphertext(self, req, tag):
        progress = self.upload_status.get_progress()[1]
        # TODO: make an ascii-art bar
        return "%.1f%%" % (100.0 * progress)

    @renderer
    def progress_encode_push(self, req, tag):
        progress = self.upload_status.get_progress()[2]
        # TODO: make an ascii-art bar
        return "%.1f%%" % (100.0 * progress)

    @renderer
    def status(self, req, tag):
        return self.upload_status.get_status()

#------------------------------------------------------------------------

class DownloadResultsRendererMixin(RateAndTimeMixin):
    # this requires a method named 'download_results'

    @renderer
    def render_servermap(self, req, tag):
        d = self.download_results()
        d.addCallback(lambda res: res.servermap)
        def _render(servermap):
            if servermap is None:
                return "None"
            l = T.ul()
            for peerid in sorted(servermap.keys()):
                peerid_s = idlib.shortnodeid_b2a(peerid)
                shares_s = ",".join(["#%d" % shnum
                                     for shnum in servermap[peerid]])
                l(T.li("[%s] has share%s: %s" % (peerid_s,
                                                 plural(servermap[peerid]),
                                                 shares_s)))
            return l
        d.addCallback(_render)
        return d

    @renderer
    def servers_used(self, req, tag):
        d = self.download_results()
        d.addCallback(lambda res: res.servers_used)
        def _got(servers_used):
            if not servers_used:
                return ""
            peerids_s = ", ".join(["[%s]" % idlib.shortnodeid_b2a(peerid)
                                   for peerid in servers_used])
            return T.li("Servers Used: ", peerids_s)
        d.addCallback(_got)
        return d

    @renderer
    def problems(self, req, tag):
        d = self.download_results()
        d.addCallback(lambda res: res.server_problems)
        def _got(server_problems):
            if not server_problems:
                return ""
            l = T.ul()
            for peerid in sorted(server_problems.keys()):
                peerid_s = idlib.shortnodeid_b2a(peerid)
                l(T.li("[%s]: %s" % (peerid_s, server_problems[peerid])))
            return T.li("Server Problems:", l)
        d.addCallback(_got)
        return d

    @renderer
    def file_size(self, req, tag):
        d = self.download_results()
        d.addCallback(lambda res: res.file_size)
        return d

    def _get_time(self, name):
        d = self.download_results()
        d.addCallback(lambda res: res.timings.get(name))
        return d

    @renderer
    def time_total(self, req, tag):
        return self._get_time("total")

    @renderer
    def time_peer_selection(self, req, tag):
        return self._get_time("peer_selection")

    @renderer
    def time_uri_extension(self, req, tag):
        return self._get_time("uri_extension")

    @renderer
    def time_hashtrees(self, req, tag):
        return self._get_time("hashtrees")

    @renderer
    def time_segments(self, req, tag):
        return self._get_time("segments")

    @renderer
    def time_cumulative_fetch(self, req, tag):
        return self._get_time("cumulative_fetch")

    @renderer
    def time_cumulative_decode(self, req, tag):
        return self._get_time("cumulative_decode")

    @renderer
    def time_cumulative_decrypt(self, req, tag):
        return self._get_time("cumulative_decrypt")

    @renderer
    def time_paused(self, ctx, data):
        return self._get_time("paused")

    def _get_rate(self, name):
        d = self.download_results()
        def _convert(r):
            file_size = r.file_size
            duration = r.timings.get(name)
            return compute_rate(file_size, duration)
        d.addCallback(_convert)
        return d

    @renderer
    def rate_total(self, req, tag):
        return self._get_rate("total")

    @renderer
    def rate_segments(self, req, tag):
        return self._get_rate("segments")

    @renderer
    def rate_fetch(self, req, tag):
        return self._get_rate("cumulative_fetch")

    @renderer
    def rate_decode(self, req, tag):
        return self._get_rate("cumulative_decode")

    @renderer
    def rate_decrypt(self, req, tag):
        return self._get_rate("cumulative_decrypt")

    @renderer
    def server_timings(self, req, tag):
        d = self.download_results()
        d.addCallback(lambda res: res.timings.get("fetch_per_server"))
        def _render(per_server):
            if per_server is None:
                return ""
            l = T.ul()
            for peerid in sorted(per_server.keys()):
                peerid_s = idlib.shortnodeid_b2a(peerid)
                times_s = ", ".join([self.render_time(None, t)
                                     for t in per_server[peerid]])
                l(T.li("[%s]: %s" % (peerid_s, times_s)))
            return T.li("Per-Server Segment Fetch Response Times: ", l)
        d.addCallback(_render)
        return d

#------------------------------------------------------------------------

def _find_overlap(events, start_key, end_key):
    """
    given a list of event dicts, return a new list in which each event
    has an extra "row" key (an int, starting at 0), and if appropriate
    a "serverid" key (ascii-encoded server id), replacing the "server"
    key. This is a hint to our JS frontend about how to overlap the
    parts of the graph it is drawing.

    we must always make a copy, since we're going to be adding keys
    and don't want to change the original objects. If we're
    stringifying serverids, we'll also be changing the serverid keys.
    """
    new_events = []
    rows = []
    for ev in events:
        ev = ev.copy()
        if ev.has_key('server'):
            ev["serverid"] = ev["server"].get_longname()
            del ev["server"]
        # find an empty slot in the rows
        free_slot = None
        for row,finished in enumerate(rows):
            if finished is not None:
                if ev[start_key] > finished:
                    free_slot = row
                    break
        if free_slot is None:
            free_slot = len(rows)
            rows.append(ev[end_key])
        else:
            rows[free_slot] = ev[end_key]
        ev["row"] = free_slot
        new_events.append(ev)
    return new_events

def _find_overlap_requests(events):
    """
    We compute a three-element 'row tuple' for each event: (serverid,
    shnum, row). All elements are ints. The first is a mapping from
    serverid to group number, the second is a mapping from shnum to
    subgroup number. The third is a row within the subgroup.

    We also return a list of lists of rowcounts, so renderers can decide
    how much vertical space to give to each row.
    """

    serverid_to_group = {}
    groupnum_to_rows = {} # maps groupnum to a table of rows. Each table
                          # is a list with an element for each row number
                          # (int starting from 0) that contains a
                          # finish_time, indicating that the row is empty
                          # beyond that time. If finish_time is None, it
                          # indicate a response that has not yet
                          # completed, so the row cannot be reused.
    new_events = []
    for ev in events:
        # DownloadStatus promises to give us events in temporal order
        ev = ev.copy()
        ev["serverid"] = ev["server"].get_longname()
        del ev["server"]
        if ev["serverid"] not in serverid_to_group:
            groupnum = len(serverid_to_group)
            serverid_to_group[ev["serverid"]] = groupnum
        groupnum = serverid_to_group[ev["serverid"]]
        if groupnum not in groupnum_to_rows:
            groupnum_to_rows[groupnum] = []
        rows = groupnum_to_rows[groupnum]
        # find an empty slot in the rows
        free_slot = None
        for row,finished in enumerate(rows):
            if finished is not None:
                if ev["start_time"] > finished:
                    free_slot = row
                    break
        if free_slot is None:
            free_slot = len(rows)
            rows.append(ev["finish_time"])
        else:
            rows[free_slot] = ev["finish_time"]
        ev["row"] = (groupnum, free_slot)
        new_events.append(ev)
    del groupnum
    # maybe also return serverid_to_group, groupnum_to_rows, and some
    # indication of the highest finish_time
    #
    # actually, return the highest rownum for each groupnum
    highest_rownums = [len(groupnum_to_rows[groupnum])
                       for groupnum in range(len(serverid_to_group))]
    return new_events, highest_rownums


def _color(server):
    h = hashlib.sha256(server.get_serverid()).digest()
    def m(c):
        return min(ord(c) / 2 + 0x80, 0xff)
    return "#%02x%02x%02x" % (m(h[0]), m(h[1]), m(h[2]))

class _EventJson(Resource, object):

    def __init__(self, download_status):
        self._download_status = download_status

    def render(self, request):
        request.setHeader("content-type", "text/plain")
        data = { } # this will be returned to the GET
        ds = self._download_status

        data["misc"] = _find_overlap(
            ds.misc_events,
            "start_time", "finish_time",
        )
        data["read"] = _find_overlap(
            ds.read_events,
            "start_time", "finish_time",
        )
        data["segment"] = _find_overlap(
            ds.segment_events,
            "start_time", "finish_time",
        )
        # TODO: overlap on DYHB isn't very useful, and usually gets in the
        # way. So don't do it.
        data["dyhb"] = _find_overlap(
            ds.dyhb_requests,
            "start_time", "finish_time",
        )
        data["block"],data["block_rownums"] =_find_overlap_requests(ds.block_requests)

        server_info = {} # maps longname to {num,color,short}
        server_shortnames = {} # maps servernum to shortname
        for d_ev in ds.dyhb_requests:
            s = d_ev["server"]
            longname = s.get_longname()
            if longname not in server_info:
                num = len(server_info)
                server_info[longname] = {"num": num,
                                         "color": _color(s),
                                         "short": s.get_name() }
                server_shortnames[str(num)] = s.get_name()

        data["server_info"] = server_info
        data["num_serverids"] = len(server_info)
        # we'd prefer the keys of serverids[] to be ints, but this is JSON,
        # so they get converted to strings. Stupid javascript.
        data["serverids"] = server_shortnames
        data["bounds"] = {"min": ds.first_timestamp, "max": ds.last_timestamp}
        return json.dumps(data, indent=1) + "\n"

#------------------------------------------------------------------------

class DownloadStatusPage(DownloadResultsRendererMixin, MultiFormatResource):

    def __init__(self, data):
        super(DownloadStatusPage, self).__init__()
        self.download_status = data
        self.putChild("event_json", _EventJson(self.download_status))

    def render_HTML(self, req):
        return renderElement(req, DownloadStatusElement(self.download_status))

class DownloadStatusElement(RateAndTimeMixin, Element):

    loader = XMLFile(FilePath(__file__).sibling("download-status.xhtml"))

    def __init__(self, download_status):
        super(DownloadStatusElement, self).__init__()
        self.download_status = download_status

    def download_results(self):
        return defer.maybeDeferred(self.download_status.get_results)

    def relative_time(self, t):
        if t is None:
            return t
        if self.download_status.first_timestamp is not None:
            return t - self.download_status.first_timestamp
        return t

    def short_relative_time(self, t):
        t = self.relative_time(t)
        if t is None:
            return ""
        return "+%.6fs" % t

    # TODO: this doesn't seem to be used.
    def render_timeline_link(self, ctx, data):
        from nevow import url
        return T.a("timeline", href=url.URL.fromContext(ctx).child("timeline"))

    def _rate_and_time(self, bytes, seconds):
        time_s = self.render_time(None, seconds)
        if seconds != 0:
            rate = self.render_rate(None, 1.0 * bytes / seconds)
            return T.span(time_s, title=rate)
        return T.span(time_s)

    @renderer
    def events(self, req, tag):
        if not self.download_status.storage_index:
            return
        srt = self.short_relative_time
        l = T.div()

        t = T.table(align="left", class_="status-download-events")
        t(T.tr(T.th("serverid"), T.th("sent"), T.th("received"),
               T.th("shnums"), T.th("RTT")))
        for d_ev in self.download_status.dyhb_requests:
            server = d_ev["server"]
            sent = d_ev["start_time"]
            shnums = d_ev["response_shnums"]
            received = d_ev["finish_time"]
            rtt = None
            if received is not None:
                rtt = received - sent
            if not shnums:
                shnums = ["-"]
            t(T.tr(style="background: %s" % _color(server))(
                (T.td(server.get_name()), T.td(srt(sent)), T.td(srt(received)),
                 T.td(",".join([str(shnum) for shnum in shnums])),
                 T.td(self.render_time(None, rtt)),
                 )))

        l(T.h2("DYHB Requests:"), t)
        l(T.br(clear="all"))

        t = T.table(align="left",class_="status-download-events")
        t(T.tr(T.th("range"), T.th("start"), T.th("finish"), T.th("got"),
               T.th("time"), T.th("decrypttime"), T.th("pausedtime"),
               T.th("speed")))
        for r_ev in self.download_status.read_events:
            start = r_ev["start"]
            length = r_ev["length"]
            bytes = r_ev["bytes_returned"]
            decrypt_time = ""
            if bytes:
                decrypt_time = self._rate_and_time(bytes, r_ev["decrypt_time"])
            speed, rtt = "",""
            if r_ev["finish_time"] is not None:
                rtt = r_ev["finish_time"] - r_ev["start_time"] - r_ev["paused_time"]
                speed = self.render_rate(None, compute_rate(bytes, rtt))
                rtt = self.render_time(None, rtt)
            paused = self.render_time(None, r_ev["paused_time"])

            t(T.tr(T.td("[%d:+%d]" % (start, length)),
                   T.td(srt(r_ev["start_time"])), T.td(srt(r_ev["finish_time"])),
                   T.td(str(bytes)), T.td(rtt),
                   T.td(decrypt_time), T.td(paused),
                   T.td(speed),
                   ))

        l(T.h2("Read Events:"), t)
        l(T.br(clear="all"))

        t = T.table(align="left",class_="status-download-events")
        t(T.tr(T.th("segnum"), T.th("start"), T.th("active"), T.th("finish"),
               T.th("range"),
               T.th("decodetime"), T.th("segtime"), T.th("speed")))
        for s_ev in self.download_status.segment_events:
            range_s = "-"
            segtime_s = "-"
            speed = "-"
            decode_time = "-"
            if s_ev["finish_time"] is not None:
                if s_ev["success"]:
                    segtime = s_ev["finish_time"] - s_ev["active_time"]
                    segtime_s = self.render_time(None, segtime)
                    seglen = s_ev["segment_length"]
                    range_s = "[%d:+%d]" % (s_ev["segment_start"], seglen)
                    speed = self.render_rate(None, compute_rate(seglen, segtime))
                    decode_time = self._rate_and_time(seglen, s_ev["decode_time"])
                else:
                    # error
                    range_s = "error"
            else:
                # not finished yet
                pass

            t(T.tr(T.td("seg%d" % s_ev["segment_number"]),
                   T.td(srt(s_ev["start_time"])),
                   T.td(srt(s_ev["active_time"])),
                   T.td(srt(s_ev["finish_time"])),
                   T.td(range_s),
                   T.td(decode_time),
                   T.td(segtime_s), T.td(speed)))

        l(T.h2("Segment Events:"), t)
        l(T.br(clear="all"))
        t = T.table(align="left",class_="status-download-events")
        t(T.tr(T.th("serverid"), T.th("shnum"), T.th("range"),
               T.th("txtime"), T.th("rxtime"),
               T.th("received"), T.th("RTT")))
        for r_ev in self.download_status.block_requests:
            server = r_ev["server"]
            rtt = None
            if r_ev["finish_time"] is not None:
                rtt = r_ev["finish_time"] - r_ev["start_time"]
            color = _color(server)
            t(T.tr(style="background: %s" % color)(
                T.td(server.get_name()), T.td(str(r_ev["shnum"])),
                T.td("[%d:+%d]" % (r_ev["start"], r_ev["length"])),
                T.td(srt(r_ev["start_time"])), T.td(srt(r_ev["finish_time"])),
                T.td(str(r_ev["response_length"] or "")),
                T.td(self.render_time(None, rtt)),
                ))

        l(T.h2("Requests:"), t)
        l(T.br(clear="all"))

        return l

    @renderer
    def results(self, req, tag):
        d = self.download_results()
        def _got_results(results):
            if results:
                return ctx.tag
            return ""
        d.addCallback(_got_results)
        return d

    @renderer
    def started(self, req, tag):
        started_s = render_time(self.download_status.get_started())
        return started_s + " (%s)" % self.download_status.get_started()

    @renderer
    def si(self, req, tag):
        si_s = base32.b2a_or_none(self.download_status.get_storage_index())
        if si_s is None:
            si_s = "(None)"
        return si_s

    @renderer
    def helper(self, req, tag):
        return {True: "Yes", False: "No"}[self.download_status.using_helper()]

    @renderer
    def total_size(self, req, tag):
        size = self.download_status.get_size()
        if size is None:
            return "(unknown)"
        return str(size)

    @renderer
    def progress(self, req, tag):
        progress = self.download_status.get_progress()
        # TODO: make an ascii-art bar
        return "%.1f%%" % (100.0 * progress)

    @renderer
    def status(self, req, tag):
        return self.download_status.get_status()

#------------------------------------------------------------------------

class RetrieveStatusPage(MultiFormatResource):

    def __init__(self, data):
        super(RetrieveStatusPage, self).__init__()
        self.retrieve_status = data

    def render_HTML(self, req):
        elem = RetrieveStatusElement(self.retrieve_status)
        return renderElement(req, elem)

class RetrieveStatusElement(Element, RateAndTimeMixin):

    loader = XMLFile(FilePath(__file__).sibling("retrieve-status.xhtml"))

    def __init__(self, retrieve_status):
        super(RetrieveStatusElement, self).__init__()
        self.retrieve_status = retrieve_status

    @renderer
    def started(self, req, tag):
        started_s = render_time(self.retrieve_status.get_started())
        return started_s

    @renderer
    def si(self, req, tag):
        si_s = base32.b2a_or_none(self.retrieve_status.get_storage_index())
        if si_s is None:
            si_s = "(None)"
        return si_s

    @renderer
    def helper(self, req, tag):
        return {True: "Yes",
                False: "No"}[self.retrieve_status.using_helper()]

    @renderer
    def current_size(self, req, tag):
        size = self.retrieve_status.get_size()
        if size is None:
            size = "(unknown)"
        return size

    @renderer
    def progress(self, req, tag):
        progress = self.retrieve_status.get_progress()
        # TODO: make an ascii-art bar
        return "%.1f%%" % (100.0 * progress)

    @renderer
    def status(self, req, tag):
        return self.retrieve_status.get_status()

    @renderer
    def encoding(self, req, tag):
        k, n = self.retrieve_status.get_encoding()
        return tag("Encoding: %s of %s" % (k, n))

    @renderer
    def problems(self, req, tag):
        problems = self.retrieve_status.get_problems()
        if not problems:
            return ""
        l = T.ul()
        for peerid in sorted(problems.keys()):
            peerid_s = idlib.shortnodeid_b2a(peerid)
            l(T.li("[%s]: %s" % (peerid_s, problems[peerid])))
        return tag("Server Problems:", l)

    def _get_rate(self, name):
        file_size = self.retrieve_status.get_size()
        duration = self.retrieve_status.timings.get(name)
        return str(compute_rate(file_size, duration))

    def _get_status_timing(self, name):
        return str(self.retrieve_status.timings.get(name))

    @renderer
    def time_total(self, req, tag):
        return tag(self._get_status_timing("total"))

    @renderer
    def rate_total(self, req, tag):
        return tag(self._get_rate("total"))

    @renderer
    def time_fetch(self, req, tag):
        return tag(self._get_status_timing("fetch"))

    @renderer
    def rate_fetch(self, req, tag):
        return tag(self._get_rate("fetch"))

    @renderer
    def time_decode(self, req, tag):
        return tag(self._get_status_timing("decode"))

    @renderer
    def rate_decode(self, req, tag):
        return tag(self._get_rate("decode"))

    @renderer
    def time_decrypt(self, req, tag):
        return tag(self._get_status_timing("decrypt"))

    @renderer
    def rate_decrypt(self, req, tag):
        return tag(self._get_rate("decrypt"))

    @renderer
    def server_timings(self, req, tag):
        per_server = self.retrieve_status.timings.get("fetch_per_server")
        if not per_server:
            return ""
        l = T.ul()
        for server in sorted(per_server.keys(), key=lambda s: s.get_name()):
            times_s = ", ".join([self.render_time(None, t)
                                 for t in per_server[server]])
            l(T.li("[%s]: %s" % (server.get_name(), times_s)))
        return T.li("Per-Server Fetch Response Times: ", l)

#------------------------------------------------------------------------

class PublishStatusPage(MultiFormatResource):

    def __init__(self, data):
        super(PublishStatusPage, self).__init__()
        self.publish_status = data

    def render_HTML(self, req):
        elem = PublishStatusElement(self.publish_status)
        return renderElement(req, elem)

class PublishStatusElement(Element, RateAndTimeMixin):

    loader = XMLFile(FilePath(__file__).sibling("publish-status.xhtml"))

    def __init__(self, publish_status):
        super(PublishStatusElement, self).__init__()
        self.publish_status = publish_status

    @renderer
    def started(self, req, tag):
        started_s = render_time(self.publish_status.get_started())
        return started_s

    @renderer
    def si(self, req, tag):
        si_s = base32.b2a_or_none(self.publish_status.get_storage_index())
        if si_s is None:
            si_s = "(None)"
        return si_s

    @renderer
    def helper(self, req, tag):
        return {True: "Yes",
                False: "No"}[self.publish_status.using_helper()]

    @renderer
    def current_size(self, req, tag):
        size = self.publish_status.get_size()
        if size is None:
            size = "(unknown)"
        return tag(str(size))

    @renderer
    def progress(self, req, tag):
        progress = self.publish_status.get_progress()
        # TODO: make an ascii-art bar
        return "%.1f%%" % (100.0 * progress)

    @renderer
    def status(self, req, tag):
        return self.publish_status.get_status()

    @renderer
    def encoding(self, req, tag):
        k, n = self.publish_status.get_encoding()
        return tag("Encoding: %s of %s" % (k, n))

    @renderer
    def sharemap(self, req, tag):
        servermap = self.publish_status.get_servermap()
        if servermap is None:
            return tag("None")
        l = T.ul()
        sharemap = servermap.make_sharemap()
        for shnum in sorted(sharemap.keys()):
            l(T.li("%d -> Placed on " % shnum,
                   ", ".join(["[%s]" % server.get_name()
                              for server in sharemap[shnum]])))
        return tag("Sharemap:", l)

    @renderer
    def problems(self, req, tag):
        problems = self.publish_status.get_problems()
        if not problems:
            return ""
        l = T.ul()
        # XXX: is this exercised? I don't think PublishStatus.problems is
        # ever populated
        for peerid in sorted(problems.keys()):
            peerid_s = idlib.shortnodeid_b2a(peerid)
            l(T.li("[%s]: %s" % (peerid_s, problems[peerid])))
        return tag("Server Problems:", l)

    def _get_rate(self, name):
        file_size = self.publish_status.get_size()
        duration = self.publish_status.timings.get(name)
        return str(compute_rate(file_size, duration))

    def _get_publish_status_timing(self, name):
        return str(self.publish_status.timings.get(name))

    @renderer
    def time_total(self, req, tag):
        return tag(self._get_publish_status_timing("total"))

    @renderer
    def rate_total(self, req, tag):
        return tag(self._get_rate("total"))

    @renderer
    def time_setup(self, req, tag):
        return tag(self._get_publish_status_timing("setup"))

    @renderer
    def time_encrypt(self, req, tag):
        return tag(self._get_publish_status_timing("encrypt"))

    @renderer
    def rate_encrypt(self, req, tag):
        return tag(self._get_rate("encrypt"))

    @renderer
    def time_encode(self, req, tag):
        return tag(self._get_publish_status_timing("encode"))

    @renderer
    def rate_encode(self, req, tag):
        return tag(self._get_rate("encode"))

    @renderer
    def time_pack(self, req, tag):
        return tag(self._get_publish_status_timing("pack"))

    @renderer
    def rate_pack(self, req, tag):
        return tag(self._get_rate("pack"))

    @renderer
    def time_sign(self, req, tag):
        return tag(self._get_publish_status_timing("sign"))

    @renderer
    def time_push(self, req, tag):
        return tag(self._get_publish_status_timing("push"))

    @renderer
    def rate_push(self, req, tag):
        return self._get_rate("push")

    @renderer
    def server_timings(self, req, tag):
        per_server = self.publish_status.timings.get("send_per_server")
        if not per_server:
            return ""
        l = T.ul()
        for server in sorted(per_server.keys(), key=lambda s: s.get_name()):
            times_s = ", ".join([self.render_time(None, t)
                                 for t in per_server[server]])
            l(T.li("[%s]: %s" % (server.get_name(), times_s)))
        return T.li("Per-Server Response Times: ", l)

#------------------------------------------------------------------------

class MapupdateStatusPage(MultiFormatResource):

    def __init__(self, data):
        super(MapupdateStatusPage, self).__init__()
        self.update_status = data

    def render_HTML(self, req):
        elem = MapupdateStatusElement(self.update_status);
        return renderElement(req, elem)

class MapupdateStatusElement(RateAndTimeMixin, Element):

    loader = XMLFile(FilePath(__file__).sibling("map-update-status.xhtml"))

    def __init__(self, update_status):
        super(MapupdateStatusElement, self).__init__()
        self.update_status = update_status

    @renderer
    def started(self, req, tag):
        started_s = render_time(self.update_status.get_started())
        return started_s

    @renderer
    def finished(self, req, tag):
        when = self.update_status.get_finished()
        if not when:
            return "not yet"
        started_s = render_time(self.update_status.get_finished())
        return started_s

    @renderer
    def si(self, req, tag):
        si_s = base32.b2a_or_none(self.update_status.get_storage_index())
        if si_s is None:
            si_s = "(None)"
        return si_s

    @renderer
    def helper(self, req, tag):
        return {True: "Yes",
                False: "No"}[self.update_status.using_helper()]

    @renderer
    def progress(self, req, tag):
        progress = self.update_status.get_progress()
        # TODO: make an ascii-art bar
        return "%.1f%%" % (100.0 * progress)

    @renderer
    def status(self, req, tag):
        return self.update_status.get_status()

    @renderer
    def problems(self, req, tag):
        problems = self.update_status.problems
        if not problems:
            return ""
        l = T.ul()
        for peerid in sorted(problems.keys()):
            peerid_s = idlib.shortnodeid_b2a(peerid)
            l(T.li("[%s]: %s" % (peerid_s, problems[peerid])))
        return tag("Server Problems:", l)

    @renderer
    def privkey_from(self, req, tag):
        server = self.update_status.get_privkey_from()
        if server:
            return tag("Got privkey from: [%s]" % server.get_name())
        else:
            return ""

    # Querying `update_status.timings` can return `None` or numeric
    # values, but twisted.web has trouble flattening the element tree
    # when such values are present.  Stringifying them seems to help,
    # hence this function.
    def _get_update_status_timing(self, name, tag):
        res = self.update_status.timings.get(name)
        if not res:
            return tag
        return tag(str(res))

    @renderer
    def time_total(self, req, tag):
        return self._get_update_status_timing("total", tag)

    @renderer
    def time_initial_queries(self, req, tag):
        return self._get_update_status_timing("initial_queries", tag)

    @renderer
    def time_cumulative_verify(self, req, tag):
        return self._get_update_status_timing("cumulative_verify", tag)

    @renderer
    def server_timings(self, req, tag):
        per_server = self.update_status.timings.get("per_server")
        if not per_server:
            return ""
        l = T.ul()
        for server in sorted(per_server.keys(), key=lambda s: s.get_name()):
            times = []
            for op,started,t in per_server[server]:
                #times.append("%s/%.4fs/%s/%s" % (op,
                #                              started,
                #                              self.render_time(None, started - self.update_status.get_started()),
                #                              self.render_time(None,t)))
                if op == "query":
                    times.append( self.render_time(None, t) )
                elif op == "late":
                    times.append( "late(" + self.render_time(None, t) + ")" )
                else:
                    times.append( "privkey(" + self.render_time(None, t) + ")" )
            times_s = ", ".join(times)
            l(T.li("[%s]: %s" % (server.get_name(), times_s)))
        return T.li("Per-Server Response Times: ", l)

#------------------------------------------------------------------------

def marshal_json(s):
    # common item data
    item = {
        "storage-index-string": base32.b2a_or_none(s.get_storage_index()),
        "total-size": s.get_size(),
        "status": s.get_status(),
    }

    # type-specific item date
    if IUploadStatus.providedBy(s):
        h, c, e = s.get_progress()
        item["type"] = "upload"
        item["progress-hash"] = h
        item["progress-ciphertext"] = c
        item["progress-encode-push"] = e

    elif IDownloadStatus.providedBy(s):
        item["type"] = "download"
        item["progress"] = s.get_progress()

    elif IPublishStatus.providedBy(s):
        item["type"] = "publish"

    elif IRetrieveStatus.providedBy(s):
        item["type"] = "retrieve"

    elif IServermapUpdaterStatus.providedBy(s):
        item["type"] = "mapupdate"
        item["mode"] = s.get_mode()

    else:
        item["type"] = "unknown"
        item["class"] = s.__class__.__name__

    return item

#------------------------------------------------------------------------

# Renders "/status" page
class Status(MultiFormatResource):

    def __init__(self, history):
        super(Status, self).__init__()
        self.history = history

    def render_HTML(self, req):
        elem = StatusElement(self._get_active_operations(),
                             self._get_recent_operations())
        return renderElement(req, elem)

    def render_JSON(self, req):
        # modern browsers now render this instead of forcing downloads
        req.setHeader("content-type", "application/json")
        data = {}
        data["active"] = active = []
        data["recent"] = recent = []

        for s in self._get_active_operations():
            active.append(marshal_json(s))

        for s in self._get_recent_operations():
            recent.append(marshal_json(s))

        return json.dumps(data, indent=1) + "\n"

    def getChild(self, path, request):
        # The "if (path is empty) return self" line should handle
        # trailing slash in request path.
        #
        # Twisted Web's documentation says this: "If the URL ends in a
        # slash, for example ``http://example.com/foo/bar/`` , the
        # final URL segment will be an empty string. Resources can
        # thus know if they were requested with or without a final
        # slash."
        if not path:
            return self

        h = self.history
        try:
            stype, count_s = path.split("-")
        except ValueError:
            raise RuntimeError(
                "no - in '{}'".format(path)
            )
        count = int(count_s)
        if stype == "up":
            for s in itertools.chain(h.list_all_upload_statuses(),
                                     h.list_all_helper_statuses()):
                # immutable-upload helpers use the same status object as a
                # regular immutable-upload
                if s.get_counter() == count:
                    return UploadStatusPage(s)
        if stype == "down":
            for s in h.list_all_download_statuses():
                if s.get_counter() == count:
                    return DownloadStatusPage(s)
        if stype == "mapupdate":
            for s in h.list_all_mapupdate_statuses():
                if s.get_counter() == count:
                    return MapupdateStatusPage(s)
        if stype == "publish":
            for s in h.list_all_publish_statuses():
                if s.get_counter() == count:
                    return PublishStatusPage(s)
        if stype == "retrieve":
            for s in h.list_all_retrieve_statuses():
                if s.get_counter() == count:
                    return RetrieveStatusPage(s)

    def _get_all_statuses(self):
        h = self.history
        return itertools.chain(h.list_all_upload_statuses(),
                               h.list_all_download_statuses(),
                               h.list_all_mapupdate_statuses(),
                               h.list_all_publish_statuses(),
                               h.list_all_retrieve_statuses(),
                               h.list_all_helper_statuses(),
                               )

    def _get_recent_operations(self):
        recent = [s
                  for s in self._get_all_statuses()
                  if not s.get_active()]
        recent.sort(lambda a, b: cmp(a.get_started(), b.get_started()))
        recent.reverse()
        return recent

    def _get_active_operations(self):
        active = [s
                  for s in self._get_all_statuses()
                  if s.get_active()]
        active.sort(lambda a, b: cmp(a.get_started(), b.get_started()))
        active.reverse()
        return active

class StatusElement(Element):

    loader = XMLFile(FilePath(__file__).sibling("status.xhtml"))

    def __init__(self, active, recent):
        super(StatusElement, self).__init__()
        self._active = active
        self._recent = recent

    @renderer
    def active_operations(self, req, tag):
        active = [self.get_op_state(op) for op in self._active]
        return SlotsSequenceElement(tag, active)

    @renderer
    def recent_operations(self, req, tag):
        active = [self.get_op_state(op) for op in self._recent]
        return SlotsSequenceElement(tag, active)

    @staticmethod
    def get_op_state(op):
        result = dict()

        started_s = render_time(op.get_started())
        result.update({"started": started_s})

        si_s = base32.b2a_or_none(op.get_storage_index())
        if si_s is None:
            si_s = "(None)"

        result.update({"si": si_s})
        result.update({"helper":
                       {True: "Yes", False: "No"}[op.using_helper()]})

        size = op.get_size()
        if size is None:
            size = "(unknown)"
        elif isinstance(size, (int, long, float)):
            size = abbreviate_size(size)

        result.update({"total_size": size})

        progress = op.get_progress()
        if IUploadStatus.providedBy(op):
            link = "up-%d" % op.get_counter()
            result.update({"type": "upload"})
            # TODO: make an ascii-art bar
            (chk, ciphertext, encandpush) = progress
            progress_s = ("hash: %.1f%%, ciphertext: %.1f%%, encode: %.1f%%" %
                          ((100.0 * chk),
                           (100.0 * ciphertext),
                           (100.0 * encandpush)))
            result.update({"progress": progress_s})
        elif IDownloadStatus.providedBy(op):
            link = "down-%d" % op.get_counter()
            result.update({"type": "download"})
            result.update({"progress": "%.1f%%" % (100.0 * progress)})
        elif IPublishStatus.providedBy(op):
            link = "publish-%d" % op.get_counter()
            result.update({"type": "publish"})
            result.update({"progress": "%.1f%%" % (100.0 * progress)})
        elif IRetrieveStatus.providedBy(op):
            result.update({"type": "retrieve"})
            link = "retrieve-%d" % op.get_counter()
            result.update({"progress": "%.1f%%" % (100.0 * progress)})
        else:
            assert IServermapUpdaterStatus.providedBy(op)
            result.update({"type": "mapupdate %s" % op.get_mode()})
            link = "mapupdate-%d" % op.get_counter()
            result.update({"progress": "%.1f%%" % (100.0 * progress)})

        result.update({"status": T.a(op.get_status(), href=link)})

        return result

#------------------------------------------------------------------------

# Renders "/helper_status" page
class HelperStatus(MultiFormatResource):

    def __init__(self, helper):
        super(HelperStatus, self).__init__()
        self._helper = helper

    def render_HTML(self, req):
        return renderElement(req, HelperStatusElement(self._helper))

    def render_JSON(self, req):
        req.setHeader("content-type", "text/plain")
        if self.helper:
            stats = self.helper.get_stats()
            return json.dumps(stats, indent=1) + "\n"
        return json.dumps({}) + "\n"

class HelperStatusElement(Element):

    loader = XMLFile(FilePath(__file__).sibling("helper.xhtml"))

    def __init__(self, helper):
        """
        :param _allmydata.immutable.offloaded.Helper helper
        """
        super(HelperStatusElement, self).__init__()
        self._helper = helper

    @renderer
    def helper_running(self, req, tag):
        # helper.get_stats() returns a dict of this form:
        #
        #   {'chk_upload_helper.active_uploads': 0,
        #    'chk_upload_helper.encoded_bytes': 0,
        #    'chk_upload_helper.encoding_count': 0,
        #    'chk_upload_helper.encoding_size': 0,
        #    'chk_upload_helper.encoding_size_old': 0,
        #    'chk_upload_helper.fetched_bytes': 0,
        #    'chk_upload_helper.incoming_count': 0,
        #    'chk_upload_helper.incoming_size': 0,
        #    'chk_upload_helper.incoming_size_old': 0,
        #    'chk_upload_helper.resumes': 0,
        #    'chk_upload_helper.upload_already_present': 0,
        #    'chk_upload_helper.upload_need_upload': 0,
        #    'chk_upload_helper.upload_requests': 0}
        #
        # If helper is running, we render the above data on the page.
        if self._helper:
            self._data = self._helper.get_stats()
            return tag
        return T.h1("No helper is running")

    @renderer
    def active_uploads(self, req, tag):
        return tag(str(self._data["chk_upload_helper.active_uploads"]))

    @renderer
    def incoming(self, req, tag):
        return "%d bytes in %d files" % (self._data["chk_upload_helper.incoming_size"],
                                         self._data["chk_upload_helper.incoming_count"])

    @renderer
    def encoding(self, req, tag):
        return "%d bytes in %d files" % (self._data["chk_upload_helper.encoding_size"],
                                         self._data["chk_upload_helper.encoding_count"])

    @renderer
    def upload_requests(self, req, tag):
        return str(self._data["chk_upload_helper.upload_requests"])

    @renderer
    def upload_already_present(self, req, tag):
        return str(self._data["chk_upload_helper.upload_already_present"])

    @renderer
    def upload_need_upload(self, req, tag):
        return str(self._data["chk_upload_helper.upload_need_upload"])

    @renderer
    def upload_bytes_fetched(self, req, tag):
        return str(self._data["chk_upload_helper.fetched_bytes"])

    @renderer
    def upload_bytes_encoded(self, req, tag):
        return str(self._data["chk_upload_helper.encoded_bytes"])

#------------------------------------------------------------------------

# Renders "/statistics" page.
class Statistics(MultiFormatResource):

    def __init__(self, provider):
        super(Statistics, self).__init__()
        self._provider = provider

    def render_HTML(self, req):
        return renderElement(req, StatisticsElement(self._provider))

    def render_JSON(self, req):
        stats = self._provider.get_stats()
        req.setHeader("content-type", "text/plain")
        return json.dumps(stats, indent=1) + "\n"

class StatisticsElement(Element):

    loader = XMLFile(FilePath(__file__).sibling("statistics.xhtml"))

    def __init__(self, provider):
        super(StatisticsElement, self).__init__()

        # provider.get_stats() returns a dict of the below form, for
        # example (there's often more data than this):
        #
        #  {
        #    'stats': {
        #      'storage_server.disk_used': 809601609728,
        #      'storage_server.accepting_immutable_shares': 1,
        #      'storage_server.disk_free_for_root': 131486851072,
        #      'storage_server.reserved_space': 1000000000,
        #      'node.uptime': 0.16520118713378906,
        #      'storage_server.disk_total': 941088460800,
        #      'cpu_monitor.total': 0.004513999999999907,
        #      'storage_server.disk_avail': 82610759168,
        #      'storage_server.allocated': 0,
        #      'storage_server.disk_free_for_nonroot': 83610759168 },
        #    'counters': {
        #      'uploader.files_uploaded': 0,
        #      'uploader.bytes_uploaded': 0,
        #       ... }
        #  }
        #
        # `counters` can be empty.
        self._stats = provider.get_stats()

    @renderer
    def load_average(self, req, tag):
        return str(self._stats["stats"].get("load_monitor.avg_load"))

    @renderer
    def peak_load(self, req, tag):
        return str(self._stats["stats"].get("load_monitor.max_load"))

    @renderer
    def uploads(self, req, tag):
        files = self._stats["counters"].get("uploader.files_uploaded", 0)
        bytes = self._stats["counters"].get("uploader.bytes_uploaded", 0)
        return ("%s files / %s bytes (%s)" %
                (files, bytes, abbreviate_size(bytes)))

    @renderer
    def downloads(self, req, tag):
        files = self._stats["counters"].get("downloader.files_downloaded", 0)
        bytes = self._stats["counters"].get("downloader.bytes_downloaded", 0)
        return ("%s files / %s bytes (%s)" %
                (files, bytes, abbreviate_size(bytes)))

    @renderer
    def publishes(self, req, tag):
        files = self._stats["counters"].get("mutable.files_published", 0)
        bytes = self._stats["counters"].get("mutable.bytes_published", 0)
        return "%s files / %s bytes (%s)" % (files, bytes,
                                             abbreviate_size(bytes))

    @renderer
    def retrieves(self, req, tag):
        files = self._stats["counters"].get("mutable.files_retrieved", 0)
        bytes = self._stats["counters"].get("mutable.bytes_retrieved", 0)
        return "%s files / %s bytes (%s)" % (files, bytes,
                                             abbreviate_size(bytes))

    @renderer
    def magic_uploader_monitored(self, req, tag):
        dirs = self._stats["counters"].get("magic_folder.uploader.dirs_monitored", 0)
        return "%s directories" % (dirs,)

    @renderer
    def magic_uploader_succeeded(self, req, tag):
        # TODO: bytes uploaded
        files = self._stats["counters"].get("magic_folder.uploader.objects_succeeded", 0)
        return "%s files" % (files,)

    @renderer
    def magic_uploader_queued(self, req, tag):
        files = self._stats["counters"].get("magic_folder.uploader.objects_queued", 0)
        return "%s files" % (files,)

    @renderer
    def magic_uploader_failed(self, req, tag):
        files = self._stats["counters"].get("magic_folder.uploader.objects_failed", 0)
        return "%s files" % (files,)

    @renderer
    def magic_downloader_succeeded(self, req, tag):
        # TODO: bytes uploaded
        files = self._stats["counters"].get("magic_folder.downloader.objects_succeeded", 0)
        return "%s files" % (files,)

    @renderer
    def magic_downloader_queued(self, req, tag):
        files = self._stats["counters"].get("magic_folder.downloader.objects_queued", 0)
        return "%s files" % (files,)

    @renderer
    def magic_downloader_failed(self, req, tag):
        files = self._stats["counters"].get("magic_folder.downloader.objects_failed", 0)
        return "%s files" % (files,)

    @renderer
    def raw(self, req, tag):
        raw = pprint.pformat(self._stats)
        return tag(raw)

#------------------------------------------------------------------------
