function disableInput(element) {
    // element.value = 0;
    element.disabled = true;
}

function enableInput(element) {
    element.disabled = false;
}

document.addEventListener('DOMContentLoaded', function () {
    var closePositionParts = document.getElementById('id_close_position_parts');
    var stopLossBreakeven = document.getElementById('id_stop_loss_breakeven');
    var tpFirstPricePercent = document.getElementById('id_tp_first_price_percent');
    var tpSecondPartPercent = document.getElementById('id_tp_second_part_percent');
    var tpSecondPricePercent = document.getElementById('id_tp_second_price_percent');
    var tpFirstPartPercent = document.getElementById('id_tp_first_part_percent');

    if (closePositionParts.checked === false) {
        disableInput(stopLossBreakeven);
        disableInput(tpFirstPricePercent);
        disableInput(tpSecondPartPercent);
        disableInput(tpSecondPricePercent);
        disableInput(tpFirstPartPercent);
    }

    closePositionParts.addEventListener('change', function () {
        if (closePositionParts.checked) {
            enableInput(stopLossBreakeven);
            enableInput(tpFirstPricePercent);
            enableInput(tpSecondPartPercent);
            enableInput(tpSecondPricePercent);
            enableInput(tpFirstPartPercent);
        } else {
            disableInput(stopLossBreakeven);
            disableInput(tpFirstPricePercent);
            disableInput(tpSecondPartPercent);
            disableInput(tpSecondPricePercent);
            disableInput(tpFirstPartPercent);
        }
    });
});
