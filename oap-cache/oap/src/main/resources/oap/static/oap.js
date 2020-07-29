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

function formatBytes(bytes, type) {
    if (type !== 'display') return bytes;
    if (bytes == 0) return '0';
    var k = 1024;
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
            var allDataFiberHitCount = 0;
            var allDataFiberMissCount = 0;
            var allDataFiberLoadCount = 0;
            var allDataLoadTime = 0;
            var allDataEvictionCount = 0;
            var allIndexFiberHitCount = 0;
            var allIndexFiberMissCount = 0;
            var allIndexFiberLoadCount = 0;
            var allIndexLoadTime = 0;
            var allIndexEvictionCount = 0;
            var indexDataCacheSeparationEnable = true;

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
                allDataFiberHitCount          += exec.dataFiberHitCount;
                allDataFiberMissCount         += exec.dataFiberMissCount;
                allDataFiberLoadCount         += exec.dataFiberLoadCount;
                allDataLoadTime          += exec.dataTotalLoadTime;
                allDataEvictionCount     += exec.dataEvictionCount;
                allIndexFiberHitCount          += exec.indexFiberHitCount;
                allIndexFiberMissCount         += exec.indexFiberMissCount;
                allIndexFiberLoadCount         += exec.indexFiberLoadCount;
                allIndexLoadTime          += exec.indexTotalLoadTime;
                allIndexEvictionCount     += exec.indexEvictionCount;
                indexDataCacheSeparationEnable = exec.indexDataCacheSeparationEnable;
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
                "allDataFiberHitCount": allDataFiberHitCount,
                "allDataFiberMissCount": allDataFiberMissCount,
                "allDataFiberLoadCount": allDataFiberLoadCount,
                "allDataLoadTime": allDataLoadTime,
                "allDataEvictionCount": allDataEvictionCount,
                "allIndexFiberHitCount": allIndexFiberHitCount,
                "allIndexFiberMissCount": allIndexFiberMissCount,
                "allIndexFiberLoadCount": allIndexFiberLoadCount,
                "allIndexLoadTime": allIndexLoadTime,
                "allIndexEvictionCount": allIndexEvictionCount
            };

            var data = {fibercachemanagers: response, "fiberCacheManagerSummary": [totalSummary]};
            $.get(createTemplateURI(appId), function (template) {

                fiberCacheManagersSummary.append(Mustache.render($(template).filter("#cms-summary-template").html(), data));

                if (indexDataCacheSeparationEnable) {
                    var selector = "#cache-separation-active-cms-table";
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
                                    return type === 'display' ? (((row.dataFiberHitCount * 100
                                        / (row.dataFiberHitCount + row.dataFiberMissCount)).toFixed(2)) === 'NaN' ?
                                        '0' : (row.dataFiberHitCount * 100 / (row.dataFiberHitCount +
                                        row.dataFiberMissCount)).toFixed(2))
                                        + '% (' + formatCount(row.dataFiberHitCount, type) + '/'
                                        + formatCount(row.dataFiberMissCount, type) + ')' : 'Nan'
                                }
                            },
                            {
                                data: function (row, type) {
                                    return type === 'display' ? (formatCount(row.dataFiberLoadCount, type)  + ' / '
                                        + formatCount(row.dataEvictionCount, type)) : 'Nan'
                                }
                            },
                            {
                                data: function (row, type) {
                                    return type === 'display' ?
                                        (formatDuration((row.dataTotalLoadTime / 1000000 / row.dataFiberLoadCount).toFixed(2))
                                         === 'NaN h' ? '0': formatDuration((row.dataTotalLoadTime / 1000000 /
                                         row.dataFiberLoadCount).toFixed(2))): 'Nan'
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
                                    return type === 'display' ? (((row.indexFiberHitCount * 100
                                        / (row.indexFiberHitCount + row.indexFiberMissCount)).toFixed(2)) === 'NaN' ?
                                        '0' : (row.indexFiberHitCount * 100 / (row.indexFiberHitCount +
                                        row.indexFiberMissCount)).toFixed(2))
                                        + '% (' + formatCount(row.indexFiberHitCount, type) + '/'
                                        + formatCount(row.indexFiberMissCount, type) + ')' : 'Nan'
                                }
                            },
                            {
                                data: function (row, type) {
                                    return type === 'display' ? (formatCount(row.indexFiberLoadCount, type)  + ' / '
                                        + formatCount(row.indexEvictionCount, type)) : 'Nan'
                                }
                            },
                            {
                                data: function (row, type) {
                                    return type === 'display' ?
                                        (formatDuration((row.indexTotalLoadTime / 1000000 / row.indexFiberLoadCount).toFixed(2))
                                         === 'NaN h' ? '0' : formatDuration((row.indexTotalLoadTime / 1000000 /
                                         row.indexFiberLoadCount).toFixed(2))): 'Nan'
                                }
                            },
                            {
                                data: function (row, type) {
                                    return type === 'display' ? (formatBytes(row.pendingFiberSize, type)  + ' / '
                                        + formatCount(row.pendingFiberCount, type)) : 'Nan'
                                }
                            },
                        ],
                        "order": [[0, "asc"]]
                    };

                    $(selector).DataTable(conf);
                    $("#cache-separation-active-cms-table tr:not(:first):first").remove();
                    $('#active-cms [data-toggle="tooltip"]').tooltip();

                    var sumSelector = "#cache-separation-summary-cms-table";
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
                                    return type === 'display' ? (((row.allDataFiberHitCount * 100
                                        / (row.allDataFiberHitCount + row.allDataFiberMissCount)).toFixed(2)) === 'NaN' ?
                                        '0' : (row.allDataFiberHitCount * 100 / (row.allDataFiberHitCount +
                                         row.allDataFiberMissCount)).toFixed(2))
                                        + '% (' + formatCount(row.allDataFiberHitCount, type) + '/'
                                        + formatCount(row.allDataFiberMissCount, type) + ')' : 'Nan'
                                }
                            },
                            {
                                data: function (row, type) {
                                    return type === 'display' ? (formatCount(row.allDataFiberLoadCount, type)  + ' / '
                                        + formatCount(row.allDataEvictionCount, type)) : 'Nan'
                                }
                            },
                            {
                                data: function (row, type) {
                                    return type === 'display' ?
                                        (formatDuration((row.allDataLoadTime / 1000000 / row.allDataFiberLoadCount).toFixed(2))
                                        === 'NaN h' ? '0' : formatDuration((row.allDataLoadTime / 1000000 /
                                        row.allDataFiberLoadCount).toFixed(2))) : 'Nan'
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
                                    return type === 'display' ? (((row.allIndexFiberHitCount * 100
                                        / (row.allIndexFiberHitCount + row.allIndexFiberMissCount)).toFixed(2)) === 'NaN' ?
                                        '0' : (row.allIndexFiberHitCount * 100  / (row.allIndexFiberHitCount +
                                         row.allIndexFiberMissCount)).toFixed(2))
                                        + '% (' + formatCount(row.allIndexFiberHitCount, type) + '/'
                                        + formatCount(row.allIndexFiberMissCount, type) + ')' : 'Nan'
                                }
                            },
                            {
                                data: function (row, type) {
                                    return type === 'display' ? (formatCount(row.allIndexFiberLoadCount, type)  + ' / '
                                        + formatCount(row.allIndexEvictionCount, type)) : 'Nan'
                                }
                            },
                            {
                                data: function (row, type) {
                                    return type === 'display' ?
                                        (formatDuration((row.allIndexLoadTime / 1000000 / row.allIndexFiberLoadCount).toFixed(2))
                                        === 'NaN h' ? '0': formatDuration((row.allIndexLoadTime / 1000000 /
                                         row.allIndexFiberLoadCount).toFixed(2))) : 'Nan'
                                }
                            },
                            {
                                data: function (row, type) {
                                    return type === 'display' ? (formatBytes(row.allPendingFiberSize, type)
                                        + ' / ' + formatCount(row.allPendingFiberCount, type)) : "Nan"
                                }
                            },
                        ],
                        "paging": false,
                        "searching": false,
                        "info": false

                    };

                    $(sumSelector).DataTable(sumConf);
                    $('#fiberCacheManagerSummary [data-toggle="tooltip"]').tooltip();
                    $("#indexdata-cache-combine").hide()
                } else {
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
                                    return type === 'display' ? (((row.dataFiberHitCount * 100
                                        / (row.dataFiberHitCount + row.dataFiberMissCount)).toFixed(2)) === 'NaN' ?
                                        '0' : (row.dataFiberHitCount * 100  / (row.dataFiberHitCount +
                                         row.dataFiberMissCount)).toFixed(2))
                                        + '% (' + formatCount(row.dataFiberHitCount, type) + '/'
                                        + formatCount(row.dataFiberMissCount, type) + ')' : 'Nan'
                                }
                            },
                            {data: 'dataFiberLoadCount', render: formatCount},
                            {
                                data: function (row, type) {
                                    return type === 'display' ?
                                        (formatDuration((row.dataTotalLoadTime / 1000000 / row.dataFiberLoadCount).toFixed(2))
                                         === 'NaN h' ? '0': formatDuration((row.dataTotalLoadTime / 1000000 /
                                          row.dataFiberLoadCount).toFixed(2))): 'Nan'
                                }
                            },
                            {data: 'dataEvictionCount', render: formatCount}
                        ],
                        "order": [[0, "asc"]]
                    };

                    $(selector).DataTable(conf);
                    $("#active-cms-table tr:not(:first):first").remove();
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
                                    return type === 'display' ? (((row.allDataFiberHitCount * 100
                                        / (row.allDataFiberHitCount + row.allDataFiberMissCount)).toFixed(2)) === 'NaN' ?
                                        '0' : (row.allDataFiberHitCount * 100 / (row.allDataFiberHitCount +
                                        row.allDataFiberMissCount)).toFixed(2))
                                        + '% (' + formatCount(row.allDataFiberHitCount, type) + '/'
                                        + formatCount(row.allDataFiberMissCount, type) + ')' : 'Nan'
                                }
                            },
                            {data: 'allDataFiberLoadCount', render: formatCount},
                            {
                                data: function (row, type) {
                                    return type === 'display' ?
                                        (formatDuration((row.allDataLoadTime / 1000000 / row.allDataFiberLoadCount).toFixed(2))
                                         === 'NaN h' ? '0' : formatDuration((row.allDataLoadTime / 1000000 /
                                         row.allDataFiberLoadCount).toFixed(2))) : 'Nan'
                                }
                            },
                            {data: "allDataEvictionCount", render: formatCount}
                        ],
                        "paging": false,
                        "searching": false,
                        "info": false

                    };

                    $(sumSelector).DataTable(sumConf);
                    $('#fiberCacheManagerSummary [data-toggle="tooltip"]').tooltip();
                    $("#indexdata-cache-separation").hide()
                }

            });
        });
    });
});
