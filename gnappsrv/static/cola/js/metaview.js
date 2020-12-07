(function(){
  document.addEventListener('DOMContentLoaded', function(){
    let $$ = selector => Array.from( document.querySelectorAll( selector ) );
    let $ = selector => document.querySelector( selector );

    let tryPromise = fn => Promise.resolve().then( fn );

    let toJson = obj => obj.json();
    let toText = obj => obj.text();

    let cy;

    let $stylesheet = "style.json";
    let getStylesheet = name => {
      let convert = res => name.match(/[.]json$/) ? toJson(res) : toText(res);

      return fetch(`/static/cola/stylesheets/${name}`).then( convert );
    };
    let applyStylesheet = stylesheet => {
      if( typeof stylesheet === typeof '' ){
        cy.style().fromString( stylesheet ).update();
      } else {
          cy.style().fromJson( stylesheet ).update();
	  cy.minZoom(0.00);
	  cy.maxZoom(10);
      }
    };
    let applyStylesheetFromSelect = () => Promise.resolve( $stylesheet ).then( getStylesheet ).then( applyStylesheet );


      
    let $dataset = "jdk_dependency.json";
    let getDataset = name => fetch(`datasets/${name}`).then( toJson );
    let getGnDataset = name => gnFetchData();
      
    let applyDataset = dataset => {
      // so new eles are offscreen
      cy.zoom(1);
      cy.pan({ x: -9999999, y: -9999999 });
      console.log('View: new Dataset is applied');
      // replace eles
      cy.elements().remove();
      cy.add( dataset );
    }
      
    let applyDatasetFromSelect = () => 
          Promise.resolve( $dataset ).then( getGnDataset ).then( applyDataset );

    ///let applyDatasetFromSelect = () => 
	///Promise.resolve( $dataset).then( getGnDataSet().then(
	    
    let $layout = "cola";
    let maxLayoutDuration = 1500;
    let layoutPadding = 50;
    let layouts = {
      cola: {
        name: 'cola',
        padding: layoutPadding,
        nodeSpacing: 12,
        edgeLengthVal: 45,
        animate: true,
        randomize: true,
        maxSimulationTime: maxLayoutDuration,
        boundingBox: { // to give cola more space to resolve initial overlaps
          x1: 0,
          y1: 0,
          x2: 1000,
          y2: 1000
        },
        edgeLength: function( e ){
          let w = e.data('weight');

          if( w == null ){
            w = 0.5;
          }

          return 45 / w;
        }
      }
    };
      
    console.log('View: Init called');  
    let prevLayout;
    let getLayout = name => Promise.resolve( layouts[ name ] );
    let applyLayout = layout => {
      if( prevLayout ){
        prevLayout.stop();
      }
      console.log('View: Apply New Layout');
      let l = prevLayout = cy.makeLayout( layout );
      return l.run().promiseOn('layoutstop');
    }
      
    let applyLayoutFromSelect = () => Promise.resolve( $layout ).then( getLayout ).then( applyLayout );

    let $algorithm = $('#algorithm');
    let getAlgorithm = (name) => {
      switch (name) {
        case 'bfs': return Promise.resolve(cy.elements().bfs.bind(cy.elements()));
        case 'dfs': return Promise.resolve(cy.elements().dfs.bind(cy.elements()));
        case 'none': return Promise.resolve(undefined);
        default: return Promise.resolve(undefined);
      }
    };
    let runAlgorithm = (algorithm) => {
      if (algorithm === undefined) {
        return Promise.resolve(undefined);
      } else {
        let options = {
          root: '#' + cy.nodes()[0].id(),
          // astar requires target; goal property is ignored for other algorithms
          goal: '#' + cy.nodes()[Math.round(Math.random() * (cy.nodes().size() - 1))].id()
        };
        return Promise.resolve(algorithm(options));
      }
    }
    let currentAlgorithm;
    let animateAlgorithm = (algResults) => {
      // clear old algorithm results
      cy.$().removeClass('highlighted start end');
      currentAlgorithm = algResults;
      if (algResults === undefined || algResults.path === undefined) {
        return Promise.resolve();
      }
      else {
        let i = 0;
        // for astar, highlight first and final before showing path
        if (algResults.distance) {
          // Among DFS, BFS, A*, only A* will have the distance property defined
          algResults.path.first().addClass('highlighted start');
          algResults.path.last().addClass('highlighted end');
          // i is not advanced to 1, so start node is effectively highlighted twice.
          // this is intentional; creates a short pause between highlighting ends and highlighting the path
        }
        return new Promise(resolve => {
          let highlightNext = () => {
            if (currentAlgorithm === algResults && i < algResults.path.length) {
              algResults.path[i].addClass('highlighted');
              i++;
              setTimeout(highlightNext, 50);
            } else {
              // resolve when finished or when a new algorithm has started visualization
              resolve();
            }
          }
          highlightNext();
        });
      }
    };
    let applyAlgorithmFromSelect = () => Promise.resolve( $algorithm.value ).then( getAlgorithm ).then( runAlgorithm ).then( animateAlgorithm );

    cy = window.cy = cytoscape({
      container: $('#cy')
    });

    //// Add panzoom   
    cy.panzoom({
       //// zoom options	  
      });

      tryPromise( applyDatasetFromSelect ).then( applyStylesheetFromSelect ).then( applyLayoutFromSelect );

      let applyDatasetChange = () => tryPromise(applyDatasetFromSelect).then(applyStylesheetFromSelect).then(applyLayoutFromSelect);
      
    $('#redo-layout').addEventListener('click', applyLayoutFromSelect);
////Disable algorithm
    ///$algorithm.addEventListener('change', applyAlgorithmFromSelect);
    ///$('#redo-algorithm').addEventListener('click', applyAlgorithmFromSelect);

    //////////////////////////////////
    $('#srchbtn').addEventListener('click', applyDatasetChange);
      
    function  gnFetchData() {

        //var srchstr = "SELECT * from customer;";
	///var srv = 'http://45.79.206.248:5050';	
	///var url = '/static/cola/js/product.json';
	var srchstr = document.getElementById('srchid').value;
	var stlbl = document.getElementById('statuslbl');
        var errlbl = document.getElementById('errorlbl');
	
	console.log('GNFetchData View: sql txt srch '+srchstr);
	var nodes_url = "/api/v1/metanodes?srchqry='"+srchstr+"'";
	var edges_url = "/api/v1/metaedges?srchqry='"+srchstr+"'";
	var nodes;
	var edges;
	var sele = '';
	var myHeaders = new Headers({
	    'Content-Type': 'text/plain',
	    'Access-Control-Allow-Origin' : '*'
	});
	
	///console.log('GNView: Fetch data1 ');
	try {
   	   return fetch(edges_url, { method: 'GET', headers: myHeaders })
	////	    fetch(edges_url, { method: 'GET',  headers: myHeaders })
	
            .then (response => response.json())
            .then (data => {
             //console.log('GNView: Fetch complete ');
		//console.log('GNView: data:'+JSON.stringify(data, null,3));
		var status = data.status;
		var statusmsg = data.statusmsg;
		var nodelen = data.nodes.length;


		if (status == "ERROR") {
                    console.log('GNView: Error status ');
                    errlbl.innerHTML = "Error getting data from database";
		    s = '[]';
		    gn_elements = JSON.parse(s);
		    return(gn_elements);
		}
		
		nodes = '';	      
		///s = '{'+"\n";
		//nodes += ' '+"\n";
		console.log('GNView: fetch complete len:'+nodelen);
		for (i=0; i < nodelen; i++) {
		    
                    if ( i > 0)
			nodes += ","+"\n";
		    
                    nodes += '{';
		    n = data.nodes[i];
                    nodes += '"data": {';
                    var idint = parseInt(n.id);
                    ///console.log('GNView node-'+i+' id '+n.id);
                    nodes+= '"id": "n'+n.id+'", ';
		    nodes+= '"idInt": '+(idint)+',';
		    nodes+= '"name": "'+n.name+'", ';
		    nodes+= '"query": true ';
                    nodes+= '},'+"\n";
		    nodes+= '"group" : "nodes",'+"\n";
		    nodes+= '"removed": false,'+"\n";
		    nodes+= '"selected": false,'+"\n";
		    nodes+= '"selectable": true,'+"\n";
		    nodes+= '"grabbable": true,'+"\n";
		    nodes+= '"locked": false'+"\n";
		    nodes+= '}';
		}
		
		///nodes += ']'+"\n";
		////////// Get edges 
		//console.log('GNView: Fetch Edges complete ');
		//console.log('GNView: data:'+JSON.stringify(data, null,3));
		var nodelen = data.edges.length;
		
		edges = '';
		
		///s = '{'+"\n";
		///edges += '['+"\n";
		//console.log('GNView: fetch Edges complete len:'+nodelen);
		for (i=0; i < nodelen; i++) {	  
		    if ( i > 0)
			edges += ",";
		    
		    edges += '{';
		    e = data.edges[i];
	            edges += '"data": {';
		    var idint = parseInt(e.id);
		    ///console.log('GNView node-'+i+' id '+n.id);
		    edges += '"id": "e'+e.id+'", ';
		    edges += '"idInt": '+(idint)+',';
		    edges += '"relname": "'+(e.type)+'" ,';
		    edges += '"source": "n'+e.source+'" ,';
		    edges += '"target": "n'+e.target+'" ,';
		    edges += '"directed": true ';
		    //edges += '"name": "'+n.name+'", ';
		    //edges += '"query": true ';
		    edges += '},'+"\n";
		    
                    edges += '"position": {},';
		    edges += '"group": "edges",';
		    edges += '"removed": false,';
		    edges += '"selected": false,';
		    edges += '"selectable": true, ';
		    edges += '"locked": false, ';
		    edges += '"grabbable": true, ';
		    edges += '"directed": true';
		    edges += '}'+"\n";		   
		}
		
		///edges += ']'+"\n";
		s = '['+"\n";
		///////s += '{'+"\n";
		s += nodes;
		s += ','+"\n";
		s += edges;
		/////s += '}'+"\n";
		s += '\n';
		s += ']'+"\n";
		////s += '}'+"\n";
		console.log('GNView nodes & edges:  '+s);
		gn_elements = JSON.parse(s);
		//gn_elements  = s;
		console.log('GnanaMetaView: fetch process is completed');
		return (gn_elements);
		////////////////////////////////
		
	    });
	} catch(error) {
	    console.log('Error caught '+error);
	}
	
    }
      ///////////////Zooming
      var zx, zy;
/////// Disable nodespace for timebeing
 //// $('#gnnodespaceid').addEventListener('change', applyNodeSpaceChange);
 /// $('#gnzoomid').addEventListener('change', applyZoomLevelChange);

  function calculateZoomCenterPoint(){
            var pan = cy.pan();
            var zoom = cy.zoom();

      zx = cy.width()/2;
      zy = cy.height()/2;
  }

      
  function   applyZoomLevelChange() {

      var cur_zoomval = document.getElementById('gnzoomid').value;
      var pan = cy.pan();
      var zoom = cy.zoom();
      var zmin = cy.minZoom();
      var zmax = cy.maxZoom();
      var zmax_f = parseFloat(zmax).toFixed(2);
      var zmin_f = parseFloat(zmin).toFixed(2);
	  
      var new_zoomlvl = (((zmax_f - zmin_f)*cur_zoomval)/100);
      
      console.log('Zoom level is  '+cur_zoomval);
      console.log('Zoom cur val:'+zoom+'   pan val:'+JSON.stringify(pan, null, 2));
      console.log('Zoom minval:'+zmin.toFixed(5)+' maxval:'+zmax_f);
      console.log('Zoom new val:'+new_zoomlvl+' ');
      //cy.zoomingEnabled(true);
      
      calculateZoomCenterPoint();
      
      cy.zoom({
	  level: new_zoomlvl,
	  renderedPosition: {x: zx, y:zy}
      });
      ///cy.viewport({
	 /// zoom: new_zoomlvl,
	  ///pan: pan
      ///});
      //if (prevLayout) {
//	  prevLayout.stop();
  //    }

      
      ///return applyLayoutFromSelect();  
  };


      
  function   applyNodeSpaceChange() {

      var nodespaceval = document.getElementById('gnnodespaceid').value;
      var pan = cy.pan();
      var zoom = cy.zoom();
      var zmin = cy.minZoom();
      var zmax = cy.maxZoom();

      console.log('Nodespace value change '+nodespaceval);
      console.log('Zoom val:'+zoom+'   pan val:'+JSON.stringify(pan, null, 2));
      console.log('Zoom minval:'+zmin+' maxval:'+zmax);
  };


      
 });

    
})();

// tooltips with jQuery
$(document).ready(() => $('.tooltip').tooltipster());
    
