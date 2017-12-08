<template>
<div id='app'>
    <header>
        <h1>QFin Crypto Ccy Status Monitor</h1>
    </header>
    <nav></nav>
    <main>
        <section>
            <simple-table caption='Bar1m Status' 
                :colnames='bar1m.colnames' :rows='bar1m.rows'></simple-table>
        </section>
        <section>
            <simple-table caption='Current Account Status' 
                :colnames='curracct.colnames' :rows='curracct.rows'></simple-table>
        </section>        
        <section class='ControlPanel'>
            <button @click='set_rebal_intv(5)'>5 min</button>
            <button @click='set_rebal_intv(30)'>30 min</button>
            <button @click='set_rebal_intv(120)'>120 min</button>
        </section>
        <section v-for='dataitem in accountChartData'>
            <div class='mainchart'>
                <h3 style='text-align:left'>{{ dataitem.caption }}</h3>
                <LineChart :width="800" :height="300" 
                           :chartData="dataitem" :options="chartOptions1"></LineChart>
            </div>
        </section>
        <section v-for='dataitem in rebalChartData'>
            <div class='mainchart'>
                <h3 style='text-align:left'>{{ dataitem.caption }}</h3>
                <LineChart :width="800" :height="200" 
                           :chartData="dataitem" :options="chartOptions2"></LineChart>
            </div>
        </section>        
    </main>
    <footer>{{ nowstamp }}</footer>
</div>
</template>

<script>
const $ = require('jquery');
import LineChart from "./line_chart";

export default {
  name: 'app'
  , data () {
        return { 
          'bar1m': {
                colnames: ['Server', 'Last Updated', 'Past 15m', 'Past 1h', 'Past 6h', 'Past 12h', 'Past 1D'],
                rows: [
                    ['Primary', 'NA', 'NA', 'NA', 'NA', 'NA', 'NA'], 
                    ['Backup',  'NA', 'NA', 'NA', 'NA', 'NA', 'NA']]
          }
        , 'curracct': {}
        , 'nowstamp': Date()
        , 'accountChartData': []
        , 'rebalChartData': []
        , 'chartOptions1': {
            responsive: false, 
            maintainAspectRatio: false,
            elements: { 
                point: { radius: 0 },
                line: {
                    tension: 0
                }
            }, 
            animation : false,
            scales: {
                xAxes: [{
                    ticks: {
                        autoSkip: true,
                        maxTicksLimit: 26
                    }
                }],
                yAxes: [
                    {
                        position: "left",
                        ticks: {
                            suggestedMin: 0
                        },
                        gridLines: {
                            display: true
                        }
                    }
                ]
            }
        }
        , 'chartOptions2': {
            responsive: false, 
            maintainAspectRatio: false,
            elements: { 
                point: { radius: 0 },
                line: {
                    tension: 0
                }
            }, 
            animation : false,
            scales: {
                xAxes: [{
                    ticks: {
                        autoSkip: true,
                        maxTicksLimit: 26
                    }
                }],
                yAxes: [
                    {
                        position: "left",
                        id: "y-axis-0",
                        ticks: {
                            suggestedMin: 0
                        },
                        gridLines: {
                            display: false
                        }
                    },
                    {
                        position: "right",
                        id: "y-axis-1",
                        gridLines: {
                            display: true
                        }
                    }
                ]
            }
        }
        , 'rebal_intv': '30'
        , 'interval': null
        , 'interval15': null
        }
    }
    , components: {
        LineChart
    }
    , methods: {
        set_rebal_intv(intv) {
            this.rebal_intv = intv.toString();
            this.load_data15()
        },
        clock_update() {
            var self = this;
            this.nowstamp = Date();
        },
        load_data15() {
            var self = this;
            $.getJSON("http://www.qfin.club:8001/bars", function(json) {
                let data = $.extend({}, json);
                self.curracct = data.Bars.curracct;
                let colnames = data.Bars.bar1m.colnames;
                let rows = [data.Bars.bar1m.rows[0], self.bar1m.rows[1]];
                self.bar1m = $.extend({}, { colnames, rows });
            }).fail(function(err) {
                console.log("Failed to load bar data from the Primary server")
            });

            $.getJSON("http://34.230.65.167:8001/bars", function(json) {
                let data = $.extend({}, json);
                let colnames = data.Bars.bar1m.colnames;
                let rows = [self.bar1m.rows[0], data.Bars.bar1m.rows[0]];
                self.bar1m = $.extend({}, { colnames, rows });
            }).fail(function(err) {
                console.log("Failed to load bar data from the Backup server")
            });

            $.getJSON("http://www.qfin.club:8001/accounts/" + self.rebal_intv, function(json) {
                let data = $.extend({}, json.Accounts);
                self.accountChartData = data;
                console.log('loaded account')
            }).fail(function(err) {
                console.log('Failed to load accounts from the Primary server');
            });

            $.getJSON("http://www.qfin.club:8001/rebals/" + self.rebal_intv, function(json) {
                let data = $.extend({}, json.Data);
                self.rebalChartData = data;
                console.log('loaded rebals')
            }).fail(function(err) {
                console.log('Failed to load rebals from the Primary server');
            });

        }
    }
    , mounted() {
        var self = this;

        this.clock_update();
        this.interval = setInterval(this.clock_update, 1000)
        
        this.load_data15();
        this.interval15 = setInterval(() => {
            this.load_data15();
        }, 15000)

    }
    ,  beforeDestroy: function(){
        clearInterval(this.interval);
        clearInterval(this.interval15);
    }
}
</script>

<style>
#app {
    padding:0;
    margin:0;
    font-family: 'Source Sans Pro', 'Helvetica Neue', Arial, sans-serif;
    font-size: 15px;
    -webkit-font-smoothing: antialiased;
    color: #34495e;

    display: grid;
    grid-template-columns: [col0] 1fr [col1] 5fr [col2] 1fr [colend];
    grid-template-rows: [row0] minmax(5em, auto) [row1] 8fr [row2] 1fr [rowend];
}


#app > header {
    background-color: #eee;
    padding: 1em;
    grid-column: col1 / col2;
    grid-row: row0 / row1;
;}

#app > main {
    grid-column: col1 / col2;
    grid-row: row1 / row2;
}

#app > main > section {
    margin: 1em 1em;
    background-color: #ffffff;
    border: #bbbbbb 1px solid;
    padding: 1em;
    text-align: center
}

#app > main > section.ControlPanel {
    border: none;
    text-align: left;
}

#app > main > section.ControlPanel > button {
    margin: 0 .5em;
}


#app > footer {
    grid-column: col1 / col2;
    grid-row: row2 / rowend;
    padding: .25em 4em;
    font-size: 11px;
    text-align: left;
}


</style>
