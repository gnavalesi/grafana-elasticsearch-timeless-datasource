import angular from 'angular';
import _ from 'lodash';
import moment from 'moment';
import { ElasticQueryBuilder } from './query_builder';
import { ElasticResponse } from './elastic_response';

export class ElasticDatasource {
  basicAuth: string;
  withCredentials: boolean;
  url: string;
  name: string;
  index: string;
  esVersion: number;
  maxConcurrentShardRequests: number;
  queryBuilder: ElasticQueryBuilder;

  /** @ngInject */
  constructor(instanceSettings, private $q, private backendSrv, private templateSrv, private timeSrv) {
    this.basicAuth = instanceSettings.basicAuth;
    this.withCredentials = instanceSettings.withCredentials;
    this.url = instanceSettings.url;
    this.name = instanceSettings.name;
    this.index = instanceSettings.index;
    this.esVersion = instanceSettings.jsonData.esVersion;
    this.maxConcurrentShardRequests = instanceSettings.jsonData.maxConcurrentShardRequests;
    this.queryBuilder = new ElasticQueryBuilder({
      esVersion: this.esVersion,
    });
  }

  private request(method, url, data?) {
    var options: any = {
      url: this.url + '/' + url,
      method: method,
      data: data,
    };

    if (this.basicAuth || this.withCredentials) {
      options.withCredentials = true;
    }
    if (this.basicAuth) {
      options.headers = {
        Authorization: this.basicAuth,
      };
    }

    return this.backendSrv.datasourceRequest(options);
  }

  private get(url) {
    return this.request('GET', this.index + url).then(function(results) {
      results.data.$$config = results.config;
      return results.data;
    });
  }

  private post(url, data) {
    return this.request('POST', url, data)
      .then(function(results) {
        results.data.$$config = results.config;
        return results.data;
      })
      .catch(err => {
        if (err.data && err.data.error) {
          throw {
            message: 'Elasticsearch error: ' + err.data.error.reason,
            error: err.data.error,
          };
        }

        throw err;
      });
  }

  testDatasource() {
    return { status: 'success', message: 'Index OK' };
  }

  getQueryHeader(searchType) {
    var query_header: any = {
      search_type: searchType,
      ignore_unavailable: true,
      index: this.index,
    };
    if (this.esVersion >= 56) {
      query_header['max_concurrent_shard_requests'] = this.maxConcurrentShardRequests;
    }
    return angular.toJson(query_header);
  }

  query(options) {
    var payload = '';
    var target;
    var sentTargets = [];

    // add global adhoc filters to timeFilter
    var adhocFilters = this.templateSrv.getAdhocFilters(this.name);

    for (var i = 0; i < options.targets.length; i++) {
      target = options.targets[i];
      if (target.hide) {
        continue;
      }

      var queryString = this.templateSrv.replace(target.query || '*', options.scopedVars, 'lucene');
      var queryObj = this.queryBuilder.build(target, adhocFilters, queryString);
      var esQuery = angular.toJson(queryObj);

      var searchType = queryObj.size === 0 && this.esVersion < 5 ? 'count' : 'query_then_fetch';
      var header = this.getQueryHeader(searchType);
      payload += header + '\n';

      payload += esQuery + '\n';
      sentTargets.push(target);
    }

    if (sentTargets.length === 0) {
      return this.$q.when([]);
    }

    payload = this.templateSrv.replace(payload, options.scopedVars);

    return this.post('_msearch', payload).then(function(res) {
      return new ElasticResponse(sentTargets, res).getTimeSeries();
    });
  }

  getFields(query) {
    return this.get('/_mapping').then(function(result) {
      var typeMap = {
        float: 'number',
        double: 'number',
        integer: 'number',
        long: 'number',
        date: 'date',
        string: 'string',
        text: 'string',
        scaled_float: 'number',
        nested: 'nested',
      };

      function shouldAddField(obj, key, query) {
        if (key[0] === '_') {
          return false;
        }

        if (!query.type) {
          return true;
        }

        // equal query type filter, or via typemap translation
        return query.type === obj.type || query.type === typeMap[obj.type];
      }

      // Store subfield names: [system, process, cpu, total] -> system.process.cpu.total
      var fieldNameParts = [];
      var fields = {};

      function getFieldsRecursively(obj) {
        for (var key in obj) {
          var subObj = obj[key];

          // Check mapping field for nested fields
          if (_.isObject(subObj.properties)) {
            fieldNameParts.push(key);
            getFieldsRecursively(subObj.properties);
          }

          if (_.isObject(subObj.fields)) {
            fieldNameParts.push(key);
            getFieldsRecursively(subObj.fields);
          }

          if (_.isString(subObj.type)) {
            var fieldName = fieldNameParts.concat(key).join('.');

            // Hide meta-fields and check field type
            if (shouldAddField(subObj, key, query)) {
              fields[fieldName] = {
                text: fieldName,
                type: subObj.type,
              };
            }
          }
        }
        fieldNameParts.pop();
      }

      for (var indexName in result) {
        var index = result[indexName];
        if (index && index.mappings) {
          var mappings = index.mappings;
          for (var typeName in mappings) {
            var properties = mappings[typeName].properties;
            getFieldsRecursively(properties);
          }
        }
      }

      // transform to array
      return _.map(fields, function(value) {
        return value;
      });
    });
  }

  getTerms(queryDef) {
    var searchType = this.esVersion >= 5 ? 'query_then_fetch' : 'count';
    var header = this.getQueryHeader(searchType);
    var esQuery = angular.toJson(this.queryBuilder.getTermsQuery(queryDef));

    esQuery = header + '\n' + esQuery + '\n';

    return this.post('_msearch?search_type=' + searchType, esQuery).then(function(res) {
      if (!res.responses[0].aggregations) {
        return [];
      }

      var buckets = res.responses[0].aggregations['1'].buckets;
      return _.map(buckets, function(bucket) {
        return {
          text: bucket.key_as_string || bucket.key,
          value: bucket.key,
        };
      });
    });
  }

  metricFindQuery(query) {
    query = angular.fromJson(query);
    if (!query) {
      return this.$q.when([]);
    }

    if (query.find === 'fields') {
      query.field = this.templateSrv.replace(query.field, {}, 'lucene');
      return this.getFields(query);
    }

    if (query.find === 'terms') {
      query.query = this.templateSrv.replace(query.query || '*', {}, 'lucene');
      return this.getTerms(query);
    }
  }

  getTagKeys() {
    return this.getFields({});
  }

  getTagValues(options) {
    return this.getTerms({ field: options.key, query: '*' });
  }
}
