import VueChartJs from 'vue-chartjs';
import { Line } from 'vue-chartjs';

export default Line.extend({
      props: ['options']
    , mixins: [VueChartJs.mixins.reactiveProp]
    , mounted () {
        this.renderChart(this.chartData, this.options)
    }
})

