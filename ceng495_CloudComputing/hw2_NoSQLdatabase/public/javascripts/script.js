$(document).ready(function() {
  var opts = {
    lines: 17,
    length: 7,
    width: 9,
    radius: 24,
    scale: 0.75,
    corners: 1.0,
    opacity: 0.15,
    rotate: 0,
    direction: 1,
    speed: 2.2,
    trail: 100,
    top: '75%',
    left: '50%',
    shadow: true,
    hwaccel: true
  }
  var spinner = new Spinner(opts).spin(document.getElementById('spinner'));
});

$('button.toggler').click(function() {
    $('button.toggler').toggleClass('hidden');
    $('.form').toggleClass('hidden');
});

function addInput(divName) {
    var newdiv1 = document.createElement('div');
    var newdiv2 = document.createElement('div');

    newdiv1.innerHTML = "<label>Extra Field Definition:</label><input type='text' name='ExtrafieldDefinition'>";
    newdiv2.innerHTML = "<label>Extra Field Description:</label><input type='text' name='ExtrafieldDescription'>";

    document.getElementById(divName).appendChild(newdiv1);
    document.getElementById(divName).appendChild(newdiv2);
}

function addRow() {
    var myName = document.getElementById("name");
    var age = document.getElementById("age");
    var table = document.getElementById("myTableData");

    var rowCount = table.rows.length;
    var row = table.insertRow(rowCount);

    row.insertCell(0).innerHTML= '<input type="button" value = "Delete" onClick="Javacsript:deleteTable(this)">';
    row.insertCell(1).innerHTML= myName.value;
    row.insertCell(2).innerHTML= age.value;
}

function deleteRow(string) {
    console.log(string);
    const data = {
      id: string.trim
    }

    $.post('http://batyr-hw2.azurewebsites.net//delete', options)
      .then(function(res) {
        console.log(res);
      }).catch(function(err) {
        console.log(err);
      });
}

function addTable(array) {
    var myTableDiv = document.getElementById("myDynamicTable");

    var table = document.createElement('TABLE');
    table.border='1';

    var tableBody = document.createElement('TBODY');
    table.appendChild(tableBody);

    for (var i=0; i<3; i++){
       var tr = document.createElement('TR');
       tableBody.appendChild(tr);
      
       for (var j=0; j<4; j++){
           var td = document.createElement('TD');
           td.width='75';
           td.appendChild(document.createTextNode("Cell " + i + "," + j));
           tr.appendChild(td);
       }
    }

    myTableDiv.appendChild(table);
}

function addEntry(array1, array2, rowindex) {
    //console.log("waygarnym="+string1);
   //console.log("waygarnym="+string2);
    console.log(array1);
    console.log(array2);

    console.log(array1[0]);

    var myTableDiv = document.getElementById("Tabler");
    var table = document.createElement('TABLE');
    table.border='1';
    var tableBody = document.createElement('TBODY');
    table.appendChild(tableBody);
//    myTableDiv.appendChild(table);
}

function editRow(index, _id) {
  var elements = $('#dmovie_' + index).children();
  var options = {id: _id};

  elements.each(function(index) {
    if (['editing', 'delete'].indexOf($(this).attr('class')) > -1) return;

    var name = $(this).find('input').attr('name');
    var value = $(this).find('input').val();
    options[name] = value;
    $(this).find('span').text(value);

    $(this).children().each(function(index) {
      $(this).toggleClass('hidden');
    });
  });

  if (event.target.id === 'save') {
    var btn = $('.dis');
    btn.prop('disabled', true);
    $('#spinner').toggleClass('hidden');

    $.post('http://batyr-hw2.azurewebsites.net/edit', options)
      .then(function(res) {
        console.log(res);
      }).catch(function(err) {
        console.log(err);
      }).always(function() {
        btn.prop('disabled', false);

        $('button.edit').each(function(index) {
          $(this).toggleClass('hidden');
        });
        $('#spinner').toggleClass('hidden');
      });
  } else {
    $('button.edit').each(function(index) {
      $(this).toggleClass('hidden');
    });
  }
}

function deleteTable(obj) {
  var index = obj.parentNode.parentNode.rowIndex;
  var table = document.getElementById("myTableData");
  table.deleteRow(index);
}
