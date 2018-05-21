import _ from 'lodash';

export class ElasticConfigCtrl {
  static templateUrl = 'public/app/plugins/datasource/elasticsearch/partials/config.html';
  current: any;

  /** @ngInject */
  constructor($scope) {
    this.current.jsonData.esVersion = this.current.jsonData.esVersion || 5;
    this.current.jsonData.maxConcurrentShardRequests = this.current.jsonData.maxConcurrentShardRequests || 256;
  }

  esVersions = [{ name: '2.x', value: 2 }, { name: '5.x', value: 5 }, { name: '5.6+', value: 56 }];
}
