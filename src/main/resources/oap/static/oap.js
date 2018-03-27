/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

function formatStatus(status, type) {
    if (type !== 'display') return status;
    if (status) {
        return "Active"
    } else {
        return "Dead"
    }
}

jQuery.extend(jQuery.fn.dataTableExt.oSort, {
    "title-numeric-pre": function (a) {
        var x = a.match(/title="*(-?[0-9\.]+)/)[1];
        return parseFloat(x);
    },

    "title-numeric-asc": function (a, b) {
        return ((a < b) ? -1 : ((a > b) ? 1 : 0));
    },

    "title-numeric-desc": function (a, b) {
        return ((a < b) ? 1 : ((a > b) ? -1 : 0));
    }
});

$(document).ajaxStop($.unblockUI);
$(document).ajaxStart(function () {
    $.blockUI({message: '<h3>Loading OAP Page...</h3>'});
});

function createTemplateURI(appId) {
    var words = document.baseURI.split('/');
    var ind = words.indexOf("proxy");
    if (ind > 0) {
        return words.slice(0, ind + 1).join('/') + '/' + appId + '/static/oap/oap-template.html';
    }
    ind = words.indexOf("history");
    if(ind > 0) {
        return words.slice(0, ind).join('/') + '/static/oap/oap-template.html';
    }
    return location.origin + "/static/oap/oap-template.html";
}

function getStandAloneppId(cb) {
    var words = document.baseURI.split('/');
    var ind = words.indexOf("proxy");
    if (ind > 0) {
        var appId = words[ind + 1];
        cb(appId);
        return;
    }
    ind = words.indexOf("history");
    if (ind > 0) {
        var appId = words[ind + 1];
        cb(appId);
        return;
    }
    //Looks like Web UI is running in standalone mode
    //Let's get application-id using REST End Point
    $.getJSON(location.origin + "/api/v1/applications", function(response, status, jqXHR) {
        if (response && response.length > 0) {
            var appId = response[0].id
            cb(appId);
            return;
        }
    });
}

function createRESTEndPoint(appId) {
    var words = document.baseURI.split('/');
    var ind = words.indexOf("proxy");
    if (ind > 0) {
        var appId = words[ind + 1];
        var newBaseURI = words.slice(0, ind + 2).join('/');
        return newBaseURI + "/api/v1/applications/" + appId + "/fibercachemanagers"
    }
    ind = words.indexOf("history");
    if (ind > 0) {
        var appId = words[ind + 1];
        var attemptId = words[ind + 2];
        var newBaseURI = words.slice(0, ind).join('/');
        if (isNaN(attemptId)) {
            return newBaseURI + "/api/v1/applications/" + appId + "/fibercachemanagers";
        } else {
            return newBaseURI + "/api/v1/applications/" + appId + "/" + attemptId + "/fibercachemanagers";
        }
    }
    return location.origin + "/api/v1/applications/" + appId + "/fibercachemanagers";
}

function formatCount(bytes, type) {
    if (type !== 'display') return bytes;
    if (bytes == 0) return '0';
    var k = 1000;
    var dm = 1;
    var sizes = ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'];
    var i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + sizes[i];
}

$(document).ready(function () {
    $.extend($.fn.dataTable.defaults, {
        stateSave: true,
        lengthMenu: [[20, 20, 20, 20, -1], [20, 20, 20, 20, "All"]],
        pageLength: 20
    });

    fiberCacheManagersSummary = $("#active-cms");

    getStandAloneppId(function (appId) {

        var endPoint = createRESTEndPoint(appId);
        $.getJSON(endPoint, function (response, status, jqXHR) {
            var summary = [];
            var allExecCnt = 0;
            var allMemoryUsed = 0;
            var allMaxMemory = 0;
            var allCacheSize = 0;
            var allCacheCount = 0;
            var allBackendCacheSize = 0;
            var allBackendCacheCount = 0;
            var allDataFiberSize = 0;
            var allDataFiberCount = 0;
            var allIndexFiberSize = 0;
            var allIndexFiberCount = 0;
            var allPendingFiberSize = 0;
            var allPendingFiberCount = 0;
            var allHitCount = 0;
            var allMissCount = 0;
            var allLoadCount = 0;
            var allLoadTime = 0;
            var allEvictionCount = 0;

            response.forEach(function (exec) {
                allExecCnt += 1;
                allMemoryUsed        += exec.memoryUsed;
                allMaxMemory         += exec.maxMemory;
                allCacheSize         += exec.cacheSize;
                allCacheCount        += exec.cacheCount;
                allBackendCacheSize  += exec.backendCacheSize;
                allBackendCacheCount += exec.backendCacheCount;
                allDataFiberSize     += exec.dataFiberSize;
                allDataFiberCount    += exec.dataFiberCount;
                allIndexFiberSize    += exec.indexFiberSize;
                allIndexFiberCount   += exec.indexFiberCount;
                allPendingFiberSize  += exec.pendingFiberSize;
                allPendingFiberCount += exec.pendingFiberCount;
                allHitCount          += exec.hitCount;
                allMissCount         += exec.missCount;
                allLoadCount         += exec.loadCount;
                allLoadTime          += exec.loadTime;
                allEvictionCount     += exec.evictionCount;
            });

            var totalSummary = {
                "execCnt": ( "Total(" + allExecCnt + ")"),
                "allMemoryUsed": allMemoryUsed,
                "allMaxMemory": allMaxMemory,
                "allCacheSize": allCacheSize,
                "allCacheCount": allCacheCount,
                "allBackendCacheSize": allBackendCacheSize,
                "allBackendCacheCount": allBackendCacheCount,
                "allDataFiberSize": allDataFiberSize,
                "allDataFiberCount": allDataFiberCount,
                "allIndexFiberSize": allIndexFiberSize,
                "allIndexFiberCount": allIndexFiberCount,
                "allPendingFiberSize": allPendingFiberSize,
                "allPendingFiberCount": allPendingFiberCount,
                "allHitCount": allHitCount,
                "allMissCount": allMissCount,
                "allLoadCount": allLoadCount,
                "allLoadTime": allLoadTime,
                "allEvictionCount": allEvictionCount
            };

            var data = {fibercachemanagers: response, "fiberCacheManagerSummary": [totalSummary]};
            $.get(createTemplateURI(appId), function (template) {

                fiberCacheManagersSummary.append(Mustache.render($(template).filter("#cms-summary-template").html(), data));
                var selector = "#active-cms-table";
                var conf = {
                    "data": response,
                    "columns": [
                        {
                            data: function (row, type) {
                                return type !== 'display' ? (isNaN(row.id) ? 0 : row.id ) : row.id;
                            }
                        },
                        {data: 'hostPort'},
                        {data: 'isActive', render: formatStatus},
                        {
                            data: function (row, type) {
                                return type === 'display' ? (formatBytes(row.memoryUsed, type) + ' / '
                                    + formatBytes(row.maxMemory, type)) : 'Nan'
                            }
                        },
                        {
                            data: function (row, type) {
                                return type === 'display' ? (formatBytes(row.cacheSize, type)  + ' / '
                                    + formatCount(row.cacheCount, type)) : 'Nan'
                            }
                        },
                        {
                            data: function (row, type) {
                                return type === 'display' ? (formatBytes(row.backendCacheSize, type)  + ' / '
                                    + formatCount(row.backendCacheCount, type)) : 'Nan'
                            }
                        },
                        {
                            data: function (row, type) {
                                return type === 'display' ? (formatBytes(row.dataFiberSize, type)  + ' / '
                                    + formatCount(row.dataFiberCount, type)) : 'Nan'
                            }
                        },
                        {
                            data: function (row, type) {
                                return type === 'display' ? (formatBytes(row.indexFiberSize, type)  + ' / '
                                    + formatCount(row.indexFiberCount, type)) : 'Nan'
                            }
                        },
                        {
                            data: function (row, type) {
                                return type === 'display' ? (formatBytes(row.pendingFiberSize, type)  + ' / '
                                    + formatCount(row.pendingFiberCount, type)) : 'Nan'
                            }
                        },
                        {
                            data: function (row, type) {
                                return type === 'display' ? (row.hitCount * 100
                                    / (row.hitCount + row.missCount)).toFixed(2)
                                    + '% (' + formatCount(row.hitCount, type) + '/'
                                    + formatCount(row.missCount, type) + ')' : 'Nan'
                            }
                        },
                        {data: 'loadCount', render: formatCount},
                        {
                            data: function (row, type) {
                                return type === 'display' ?
                                    formatDuration((row.loadTime / 1000000 / row.loadCount).toFixed(2)) : 'Nan'
                            }
                        },
                        {data: 'evictionCount', render: formatCount}
                    ],
                    "order": [[0, "asc"]]
                };

                $(selector).DataTable(conf);
                $('#active-cms [data-toggle="tooltip"]').tooltip();

                var sumSelector = "#summary-cms-table";
                var sumConf = {
                    "data": [totalSummary],
                    "columns": [
                        {
                            data: 'execCnt',
                            "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
                                $(nTd).css('font-weight', 'bold');
                            }
                        },
                        {
                            data: function (row, type) {
                                return type === 'display' ? (formatBytes(row.allMemoryUsed, type)
                                    + ' / ' + formatBytes(row.allMaxMemory, type)) : 'Nan';
                            }
                        },
                        {
                            data: function (row, type) {
                                return type === 'display' ? (formatBytes(row.allCacheSize, type)
                                    + ' / ' + formatCount(row.allCacheCount, type)) : 'Nan'
                            }
                        },
                        {
                            data: function (row, type) {
                                return type === 'display' ? (formatBytes(row.allBackendCacheSize, type)
                                    + ' / ' + formatCount(row.allBackendCacheCount, type)) : 'Nan'
                            }
                        },
                        {
                            data: function (row, type) {
                                return type === 'display' ? (formatBytes(row.allDataFiberSize, type)
                                    + ' / ' + formatCount(row.allDataFiberCount, type)) : 'Nan'
                            }
                        },
                        {
                            data: function (row, type) {
                                return type === 'display' ? (formatBytes(row.allIndexFiberSize, type)
                                    + ' / ' + formatCount(row.allIndexFiberCount, type)) : "Nan"
                            }
                        },
                        {
                            data: function (row, type) {
                                return type === 'display' ? (formatBytes(row.allPendingFiberSize, type)
                                    + ' / ' + formatCount(row.allPendingFiberCount, type)) : "Nan"
                            }
                        },
                        {
                            data: function (row, type) {
                                return type === 'display' ? (row.allHitCount * 100
                                    / (row.allHitCount + row.allMissCount)).toFixed(2)
                                    + '% (' + formatCount(row.allHitCount, type) + '/'
                                    + formatCount(row.allMissCount, type) + ')' : 'Nan'
                            }
                        },
                        {data: 'allLoadCount', render: formatCount},
                        {
                            data: function (row, type) {
                                return type === 'display' ?
                                    formatDuration((row.allLoadTime / 1000000 / row.allLoadCount).toFixed(2)) : 'Nan'
                            }
                        },
                        {data: "allEvictionCount", render: formatCount}
                    ],
                    "paging": false,
                    "searching": false,
                    "info": false

                };

                $(sumSelector).DataTable(sumConf);
                $('#fiberCacheManagerSummary [data-toggle="tooltip"]').tooltip();

            });
        });
    });
});
