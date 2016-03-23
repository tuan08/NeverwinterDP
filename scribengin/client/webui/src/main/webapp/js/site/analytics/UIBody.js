define([
  'jquery',
  'underscore', 
  'backbone',
  'ui/UIBreadcumbs',
  'site/analytics/UIMousePlayback',
  'site/analytics/UIAction',
  'site/analytics/UIUserInfo',
  'site/analytics/UIWebEvent',
  'text!site/analytics/UIBody.jtpl'
], function($, _, Backbone, UIBreadcumbs, UIMousePlayback, UIAction, UIUserInfo, UIWebEvent, Template) {
  var UIBody = Backbone.View.extend({
    el: $("#UIBody"),
    
    initialize: function () {
      _.bindAll(this, 'render') ;
    },
    
    _template: _.template(Template),

    render: function() {
      var params = { } ;
      $(this.el).html(this._template(params));
    },

    events: {
      'click .onSelectDiagram':  'onSelectDiagram',

      'click .onSelectUserInfoInput':  'onSelectUserInfoInput',
      'click .onMouseReplay':  'onMouseReplay',
      'click .onAction': 'onAction',
      'click .onSelectWebEventInput':  'onSelectWebEventInput',
    },

    onSelectDiagram: function(evt) {
      this.render();
    },

    onSelectUserInfoInput: function(evt) {
      var uiUserInfo = new UIUserInfo();
      this._workspace(uiUserInfo);
    },

    onSelectWebEventInput: function(evt) {
      var uiWebEvent = new UIWebEvent();
      this._workspace(uiWebEvent);
    },
    
    onMouseReplay: function(evt) {
      var uiMousePlayback = new UIMousePlayback();
      this._workspace(uiMousePlayback);
    },

    onAction: function(evt) {
      var uiAction = new UIAction();
      this._workspace(uiAction);
    },

    _workspace: function(uicomp) {
      $('#UIWorkspace').empty();
      $('#UIWorkspace').unbind();
      var uiContainer = new UIBreadcumbs({el: null}) ;
      uiContainer.setElement($('#UIWorkspace')).render();
      uiContainer.add(uicomp) ;
    }
  });
  
  return new UIBody();
});