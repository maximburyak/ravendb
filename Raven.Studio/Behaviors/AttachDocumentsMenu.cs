using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Interactivity;
using System.Windows.Media;
using System.Windows.Shapes;
using SL4PopupMenu;

namespace Raven.Studio.Behaviors
{
	public class AttachDocumentsMenu : Behavior<ListBox>
	{
		private PopupMenu menu;

		protected override void OnAttached()
		{
			CreateMenu();

			//TargetMenu.OpenNextTo(MenuOrientationTypes.MouseBottomRight, null, true, true);

			base.OnAttached();
		}

		private void CreateMenu()
		{
			menu = new PopupMenu();
			menu.AddItem("Edit Document", null);
			menu.AddItem("Copy Document Id to Clipboard", null);
			menu.AddSeparator();
			menu.AddItem("Delete Document", null);

			menu.Opening += OpenOnlyOnDocumentItem;
			menu.AddTrigger(TriggerTypes.RightClick, AssociatedObject);

			//        <popupMenu:PopupMenu x:Name="menu">
			//    <ListBox>
			//        <popupMenu:PopupMenuItem Header="Edit Document"
			//                                 cm:Action.TargetWithoutContext="EditDocument"
			//                                 cm:Message.Attach="[Click]=[EditDocument($selectedItems)]" />
			//        <popupMenu:PopupMenuItem Header="Copy Document Id to Clipboard" />
			//        <popupMenu:PopupMenuSeparator />
			//        <popupMenu:PopupMenuItem Header="Delete Document" />
			//    </ListBox>
			//</popupMenu:PopupMenu>
		}

		private void OpenOnlyOnDocumentItem(object sender, RoutedEventArgs e)
		{
			var ele = e.OriginalSource as DependencyObject;		// ListBoxItem | ContentControl | Control | FrameworkElement
			menu.IsOpeningCancelled = true;
			while (ele != null && (ele is ScrollViewer) == false)
			{
				if (ele is ListBoxItem)
				{
					menu.IsOpeningCancelled = false;
					break;
				}
				ele = VisualTreeHelper.GetParent(ele);
			}
		}

		protected override void OnDetaching()
		{
			base.OnDetaching();
		}
	}
}