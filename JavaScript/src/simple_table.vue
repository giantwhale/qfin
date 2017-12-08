<template>
    <div class='simple-table'>
        <table>
            <caption>{{ caption }}</caption>
            <thead>
                <tr>
                    <th v-for="col in colnames">{{ col }}</th>
                </tr>
            </thead>
            <tbody>
                <tr v-for="row in rows">
                    <td v-for='item in row'>{{ item }}</td>
                </tr>
            </tbody>
        </table>
    </div>
</template>


<script>
export default {
    props: ['caption', 'colnames', 'rows']
}
</script>


<style>
.simple-table table { border-collapse:collapse }

.simple-table caption { 
    caption-side: left;
    font-size: 1.5em;
}

.simple-table th  {
    background-color: #00688B;
    color: #ffffff;
    border: 0;
    margin: 0;
    padding: .5em 1.5em;
    border-bottom: #aaaaaa 1px solid;
    border-top: #aaaaaa 1px solid;
}


.simple-table td  {
    border: 0;
    margin: 0;
    padding: .25em 1.5em;
    border-bottom: #aaaaaa 1px solid;
    border-top: #aaaaaa 1px solid;
}

.simple-table tr:nth-of-type(even) {
    background-color: #eee;
}
</style>