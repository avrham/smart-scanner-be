"""Structurally faithful excerpts of the two macro calendar pages.

Trimmed by hand from the live markup captured on 2026-08-30, keeping every
element the parsers actually key on and nothing else. Real pages are ~165 KB
and ~76 KB; committing them whole would make a layout diff unreadable and would
tie the suite to text neither parser looks at.

The awkward shapes are deliberately preserved because they are the ones that
break parsers: a meeting spanning a month boundary, a Summary-of-Economic-
Projections asterisk, a future meeting with NO press-conference link, the page
FOOTER whose navigation contains a link matching the press-conference pattern,
a BEA table that rolls from December into January, and BEA's regional GDP
releases whose titles also begin with "GDP".
"""

FOMC_CALENDAR_HTML = """
<html><body>
<div class="panel panel-default">
  <div class="panel-heading"><h4><a id="42828">2026 FOMC Meetings</a></h4></div>

  <div class="row fomc-meeting">
    <div class="fomc-meeting__month col-md-2"><strong>January</strong></div>
    <div class="fomc-meeting__date col-lg-1">27-28</div>
    <div class="col-lg-2">
      <strong>Statement:</strong><br>
      <a href="/newsevents/pressreleases/monetary20260128a.htm">HTML</a>
    </div>
    <div class="col-lg-3">
      <a href="/monetarypolicy/fomcpressconf20260128.htm">Press Conference</a>
    </div>
  </div>

  <div class="fomc-meeting--shaded row fomc-meeting">
    <div class="fomc-meeting--shaded fomc-meeting__month col-md-2"><strong>March</strong></div>
    <div class="fomc-meeting__date col-lg-1">17-18*</div>
    <div class="col-lg-3">
      <a href="/monetarypolicy/fomcpresconf20260318.htm">Press Conference</a>
      <strong>Projection Materials</strong>
    </div>
  </div>

  <div class="row fomc-meeting">
    <div class="fomc-meeting__month col-md-2"><strong>September</strong></div>
    <div class="fomc-meeting__date col-lg-1">15-16*</div>
    <div class="col-lg-2"></div>
  </div>

  <div class="row fomc-meeting">
    <div class="fomc-meeting__month col-md-2"><strong>December/January</strong></div>
    <div class="fomc-meeting__date col-lg-1">31-1</div>
    <div class="col-lg-2"></div>
  </div>

  <div class="panel-footer">* Meeting associated with a Summary of Economic Projections.</div>
</div>

<div class="row">
  <div class="col-md-12"><p>Note: A two-day meeting is scheduled for January 25-26, 2028.</p></div>
</div>
<footer class="container footer">
  <ul><li><a href="/monetarypolicy/fomcpressconf20250101.htm">Press Conferences</a></li></ul>
</footer>
</body></html>
"""

BEA_SCHEDULE_HTML = """
<html><body>
<table class="table" id="release-schedule-table">
  <thead><tr><th id="view-field-scheduled-release-date-1-table-column">Year 2026</th>
  <th></th><th>Release</th></tr></thead>
  <tbody>
    <tr>
      <td class="scheduled-date"><div class="release-date">September 30</div>
      <small class="text-muted">8:30 AM</small></td>
      <td></td>
      <td class="release-title views-field">GDP (Third Estimate), Industries, Corporate Profits, State GDP, and State Personal Income, 2nd Quarter 2026        </td>
    </tr>
    <tr>
      <td class="scheduled-date"><div class="release-date">September 30</div>
      <small class="text-muted">8:30 AM</small></td>
      <td></td>
      <td class="release-title views-field">Personal Income and Outlays, August 2026        </td>
    </tr>
    <tr>
      <td class="scheduled-date"><div class="release-date">October 6</div>
      <small class="text-muted">8:30 AM</small></td>
      <td></td>
      <td class="release-title views-field">U.S. International Trade in Goods and Services, August 2026        </td>
    </tr>
    <tr>
      <td class="scheduled-date"><div class="release-date">December 2</div>
      <small class="text-muted">8:30 AM</small></td>
      <td></td>
      <td class="release-title views-field">GDP by County and Personal Income by County, 2025        </td>
    </tr>
    <tr>
      <td class="scheduled-date"><div class="release-date">January 29</div>
      <small class="text-muted">8:30 AM</small></td>
      <td></td>
      <td class="release-title views-field">GDP (Advance Estimate), 4th Quarter 2026        </td>
    </tr>
  </tbody>
</table>
</body></html>
"""

#: A page that still returns HTTP 200 and no longer contains a calendar. This
#: is the failure mode that matters: an empty parse must be reported as
#: `unavailable`, never as "there are no meetings scheduled".
FOMC_LAYOUT_CHANGED_HTML = "<html><body><h1>FOMC Calendars</h1></body></html>"
BEA_LAYOUT_CHANGED_HTML = "<html><body><p>Release schedule</p></body></html>"
