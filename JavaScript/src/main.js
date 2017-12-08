import Vue from 'vue';
import App from './App.vue';
import SimpleTable from './simple_table.vue';

Vue.component('simple-table', SimpleTable);

new Vue({
  el: '#app',
  render: h => h(App)
})
